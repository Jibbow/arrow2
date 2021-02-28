// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::cmp::{max, min};
use std::mem::replace;

use parquet::column::{page::PageReader, reader::ColumnReaderImpl};
use parquet::data_type::DataType;
use parquet::schema::types::ColumnDescPtr;

use crate::buffer::Bitmap;
use crate::error::Result;

const MIN_BATCH_SIZE: usize = 1024;

/// A `RecordReader` is a stateful column reader that delimits semantic records.
pub struct RecordReader<T: DataType> {
    column_desc: ColumnDescPtr,

    records: Vec<T::T>,
    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
    null_bitmap: Option<Vec<bool>>,
    column_reader: Option<ColumnReaderImpl<T>>,

    /// Number of records accumulated in records
    num_records: usize,
    /// Number of values `num_records` contains.
    num_values: usize,

    values_seen: usize,
    /// Starts from 1, number of values have been written to buffer
    values_written: usize,
    in_middle_of_record: bool,
}

impl<T: DataType> RecordReader<T> {
    pub fn new(column_schema: ColumnDescPtr) -> Self {
        let (def_levels, null_map) = if column_schema.max_def_level() > 0 {
            (
                Some(Vec::with_capacity(MIN_BATCH_SIZE)),
                Some(Vec::with_capacity(MIN_BATCH_SIZE)),
            )
        } else {
            (None, None)
        };

        let rep_levels = if column_schema.max_rep_level() > 0 {
            Some(Vec::with_capacity(MIN_BATCH_SIZE))
        } else {
            None
        };

        Self {
            records: Vec::with_capacity(MIN_BATCH_SIZE),
            def_levels,
            rep_levels,
            null_bitmap: null_map,
            column_reader: None,
            column_desc: column_schema,
            num_records: 0,
            num_values: 0,
            values_seen: 0,
            values_written: 0,
            in_middle_of_record: false,
        }
    }

    /// Set the current page reader.
    pub fn set_page_reader(&mut self, page_reader: Box<dyn PageReader>) {
        self.column_reader = Some(ColumnReaderImpl::new(self.column_desc.clone(), page_reader));
    }

    /// Try to read `num_records` of column data into internal buffer.
    ///
    /// # Returns
    ///
    /// Number of actual records read.
    pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
        if self.column_reader.is_none() {
            return Ok(0);
        }

        let mut records_read = 0;

        // Used to mark whether we have reached the end of current
        // column chunk
        let mut end_of_column = false;

        loop {
            // Try to find some records from buffers that has been read into memory
            // but not counted as seen records.
            records_read += self.split_records(num_records - records_read);

            // Since page reader contains complete records, so if we reached end of a
            // page reader, we should reach the end of a record
            if end_of_column && self.values_seen >= self.values_written && self.in_middle_of_record
            {
                self.num_records += 1;
                self.num_values = self.values_seen;
                self.in_middle_of_record = false;
                records_read += 1;
            }

            if (records_read >= num_records) || end_of_column {
                break;
            }

            let batch_size = max(num_records - records_read, MIN_BATCH_SIZE);

            // Try to more value from parquet pages
            let values_read = self.read_one_batch(batch_size)?;
            if values_read < batch_size {
                end_of_column = true;
            }
        }

        Ok(records_read)
    }

    /// Returns number of records stored in buffer.
    pub fn num_records(&self) -> usize {
        self.num_records
    }

    /// Return number of values stored in buffer.
    /// If the parquet column is not repeated, it should be equals to `num_records`,
    /// otherwise it should be larger than or equal to `num_records`.
    pub fn num_values(&self) -> usize {
        self.num_values
    }

    /// Returns definition level data.
    /// The implementation has side effects. It will create a new buffer to hold those
    /// definition level values that have already been read into memory but not counted
    /// as record values, e.g. those from `self.num_values` to `self.values_written`.
    pub fn consume_def_levels(&mut self) -> Result<Option<Vec<i16>>> {
        let new_buffer = if let Some(ref mut buffer) = &mut self.def_levels {
            let num_left_values = self.values_written - self.num_values;

            let mut new_buffer = Vec::with_capacity(num_left_values);

            let left_levels = &buffer.as_mut_slice()[self.num_values..];
            new_buffer.extend_from_slice(&left_levels[0..num_left_values]);

            buffer.resize(self.num_values, 0);

            Some(new_buffer)
        } else {
            None
        };

        Ok(replace(&mut self.def_levels, new_buffer).map(|x| x.into()))
    }

    /// Return repetition level data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_rep_levels(&mut self) -> Result<Option<Vec<i16>>> {
        let new_buffer = if let Some(ref mut buffer) = &mut self.rep_levels {
            let num_left_values = self.values_written - self.num_values;
            let mut new_buffer = Vec::with_capacity(num_left_values);

            let left_levels = &buffer.as_mut_slice()[self.num_values..];
            new_buffer.extend_from_slice(&left_levels[0..num_left_values]);

            buffer.resize(self.num_values, 0);

            Some(new_buffer)
        } else {
            None
        };

        Ok(replace(&mut self.rep_levels, new_buffer).map(|x| x.into()))
    }

    /// Returns currently stored buffer data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_record_data(&mut self) -> Result<Vec<T::T>> {
        let buffer = &mut self.records;

        let num_left_values = self.values_written - self.num_values;
        let mut new_buffer = Vec::with_capacity(num_left_values);

        let left_levels = &buffer.as_mut_slice()[self.num_values..];
        new_buffer.extend_from_slice(&left_levels[0..num_left_values]);

        buffer.resize(self.num_values, T::T::default());

        Ok(replace(&mut self.records, new_buffer).into())
    }

    /// Returns currently stored null bitmap data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_bitmap_buffer(&mut self) -> Result<Option<Vec<bool>>> {
        // TODO: Optimize to reduce the copy
        let new_buffer = if let Some(ref mut buffer) = &mut self.null_bitmap {
            assert!(self.column_desc.max_def_level() > 0);

            let num_left_values = self.values_written - self.num_values;
            let mut new_buffer = Vec::with_capacity(num_left_values);

            let left_levels = &buffer.as_mut_slice()[self.num_values..];
            new_buffer.extend_from_slice(&left_levels[0..num_left_values]);

            buffer.resize(self.num_values, false);

            Some(new_buffer)
        } else {
            None
        };
        Ok(replace(&mut self.null_bitmap, new_buffer))
    }

    /// Reset state of record reader.
    /// Should be called after consuming data, e.g. `consume_rep_levels`,
    /// `consume_rep_levels`, `consume_record_data` and `consume_bitmap_buffer`.
    pub fn reset(&mut self) {
        self.values_written -= self.num_values;
        self.num_records = 0;
        self.num_values = 0;
        self.values_seen = 0;
        self.in_middle_of_record = false;
    }

    /// Returns bitmap data.
    pub fn consume_bitmap(&mut self) -> Result<Option<Bitmap>> {
        self.consume_bitmap_buffer()
            .map(|buffer| buffer.map(|x| x.into_iter().collect()))
    }

    /// Try to read one batch of data.
    fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
        // Reserve spaces
        self.records
            .resize(self.records.len() + batch_size, T::T::default());
        if let Some(ref mut buf) = self.rep_levels {
            buf.resize(buf.len() + batch_size, 0);
        }
        if let Some(ref mut buf) = self.def_levels {
            buf.resize(buf.len() + batch_size, 0);
        }

        let values_written = self.values_written;

        let values = &mut self.records[values_written..];

        let def_levels = self
            .def_levels
            .as_mut()
            .map(|def_levels| &mut def_levels[values_written..]);

        let rep_levels = self
            .rep_levels
            .as_mut()
            .map(|rep_levels| &mut rep_levels[values_written..]);

        let (values_read, levels_read) = self
            .column_reader
            .as_mut()
            .unwrap()
            .read_batch(batch_size, def_levels, rep_levels, values)?;

        // get new references for the def levels.
        let def_levels = self.def_levels.as_ref().map(|buf| {
            let (prefix, def_levels, suffix) = unsafe { buf.as_slice().align_to::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            &def_levels[values_written..]
        });

        let max_def_level = self.column_desc.max_def_level();

        if values_read < levels_read {
            let def_levels =
                def_levels.expect("Definition levels should exist when data is less than levels!");

            // Fill spaces in column data with default values
            let mut values_pos = values_read;
            let mut level_pos = levels_read;

            while level_pos > values_pos {
                if def_levels[level_pos - 1] == max_def_level {
                    // This values is not empty
                    // We use swap rather than assign here because T::T doesn't
                    // implement Copy
                    values.swap(level_pos - 1, values_pos - 1);
                    values_pos -= 1;
                } else {
                    values[level_pos - 1] = T::T::default();
                }

                level_pos -= 1;
            }
        }

        // Fill in bitmap data
        if let Some(null_buffer) = self.null_bitmap.as_mut() {
            let def_levels =
                def_levels.expect("Definition levels should exist when data is less than levels!");
            (0..levels_read).for_each(|idx| null_buffer.push(def_levels[idx] == max_def_level));
        }

        let values_read = max(values_read, levels_read);
        self.set_values_written(self.values_written + values_read);
        Ok(values_read)
    }

    /// Split values into records according repetition definition and returns number of
    /// records read.
    fn split_records(&mut self, records_to_read: usize) -> usize {
        let rep_levels = self
            .rep_levels
            .as_ref()
            .map(|rep_levels| rep_levels.as_slice());

        match rep_levels {
            Some(buf) => {
                let mut records_read = 0;

                while (self.values_seen < self.values_written) && (records_read < records_to_read) {
                    if buf[self.values_seen] == 0 {
                        if self.in_middle_of_record {
                            records_read += 1;
                            self.num_records += 1;
                            self.num_values = self.values_seen;
                        }
                        self.in_middle_of_record = true;
                    }
                    self.values_seen += 1;
                }

                records_read
            }
            None => {
                let records_read = min(records_to_read, self.values_written - self.values_seen);
                self.num_records += records_read;
                self.num_values += records_read;
                self.values_seen += records_read;
                self.in_middle_of_record = false;

                records_read
            }
        }
    }

    fn set_values_written(&mut self, new_values_written: usize) {
        self.values_written = new_values_written;
        self.records.resize(self.values_written, T::T::default());

        if let Some(ref mut buf) = self.rep_levels {
            buf.resize(self.values_written, 0)
        };

        if let Some(ref mut buf) = self.def_levels {
            buf.resize(self.values_written, 0)
        };
    }
}
