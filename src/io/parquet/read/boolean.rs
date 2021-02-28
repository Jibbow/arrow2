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

use std::any::Any;
use std::result::Result::Ok;
use std::sync::Arc;
use std::vec::Vec;

use parquet::data_type::BoolType;

use parquet::column::page::PageIterator;
use parquet::schema::types::ColumnDescPtr;

use crate::array::*;
use crate::error::Result;
use crate::{
    buffer::{Bitmap, MutableBitmap},
    datatypes::DataType,
};

use super::super::record_reader::RecordReader;
use super::ArrayReader;

type ArrayRef = Arc<dyn Array>;

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct BooleanArrayReader {
    data_type: DataType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: RecordReader<BoolType>,
}

impl BooleanArrayReader {
    /// Construct primitive array reader.
    pub fn new(
        mut pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        data_type: DataType,
    ) -> Result<Self> {
        let mut record_reader = RecordReader::<BoolType>::new(column_desc.clone());
        if let Some(page_reader) = pages.next() {
            record_reader.set_page_reader(page_reader?);
        }

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader,
        })
    }
}

impl ArrayReader for BooleanArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let mut records_read = 0usize;
        while records_read < batch_size {
            let records_to_read = batch_size - records_read;

            // NB can be 0 if at end of page
            let records_read_once = self.record_reader.read_records(records_to_read)?;
            records_read += records_read_once;

            // Record reader exhausted
            if records_read_once < records_to_read {
                if let Some(page_reader) = self.pages.next() {
                    // Read from new page reader
                    self.record_reader.set_page_reader(page_reader?);
                } else {
                    // Page reader also exhausted
                    break;
                }
            }
        }

        let values = self.record_reader.consume_record_data()?;
        let validity = self.record_reader.consume_bitmap_buffer()?;

        assert_eq!(values.len(), self.record_reader.num_values());
        assert_eq!(
            validity.as_ref().map(|x| x.len()).unwrap_or(values.len()),
            values.len()
        );

        let validity = validity.as_ref().and_then(|x| {
            unsafe { MutableBitmap::from_trusted_len_iter(x.into_iter().map(|x| *x)) }.into()
        });
        let values = unsafe { Bitmap::from_trusted_len_iter(values.into_iter()) };

        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels()?;
        self.rep_levels_buffer = self.record_reader.consume_rep_levels()?;
        self.record_reader.reset();
        Ok(Arc::new(BooleanArray::from_data(values, validity)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_ref().map(|buf| buf.as_ref())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|buf| buf.as_ref())
    }
}
