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

//! Contains reader which reads parquet data into arrow array.

use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::FileReader;

use crate::array::StructArray;
use crate::datatypes::{DataType, Schema};
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::array_reader::build_array_reader;
use super::read::{ArrayReader, StructArrayReader};
use super::schema::{
    parquet_to_arrow_schema, parquet_to_arrow_schema_by_columns,
    parquet_to_arrow_schema_by_root_columns,
};

/// Arrow reader api.
/// With this api, user can get arrow schema from parquet file, and read parquet data
/// into arrow arrays.
pub trait ArrowReader {
    type RecordReader: RecordBatchReader;

    /// Read parquet schema and convert it into arrow schema.
    fn get_schema(&mut self) -> Result<Schema>;

    /// Read parquet schema and convert it into arrow schema.
    /// This schema only includes columns identified by `column_indices`.
    /// To select leaf columns (i.e. `a.b.c` instead of `a`), set `leaf_columns = true`
    fn get_schema_by_columns<T>(&mut self, column_indices: T, leaf_columns: bool) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>;

    /// Returns record batch reader from whole parquet file.
    ///
    /// # Arguments
    ///
    /// `batch_size`: The size of each record batch returned from this reader. Only the
    /// last batch may contain records less than this size, otherwise record batches
    /// returned from this reader should contains exactly `batch_size` elements.
    fn get_record_reader(&mut self, batch_size: usize) -> Result<Self::RecordReader>;

    /// Returns record batch reader whose record batch contains columns identified by
    /// `column_indices`.
    ///
    /// # Arguments
    ///
    /// `column_indices`: The columns that should be included in record batches.
    /// `batch_size`: Please refer to `get_record_reader`.
    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<Self::RecordReader>
    where
        T: IntoIterator<Item = usize>;
}

pub struct ParquetFileArrowReader {
    file_reader: Arc<dyn FileReader>,
}

impl ArrowReader for ParquetFileArrowReader {
    type RecordReader = ParquetRecordBatchReader;

    fn get_schema(&mut self) -> Result<Schema> {
        let file_metadata = self.file_reader.metadata().file_metadata();
        parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
    }

    fn get_schema_by_columns<T>(&mut self, column_indices: T, leaf_columns: bool) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>,
    {
        let file_metadata = self.file_reader.metadata().file_metadata();
        if leaf_columns {
            parquet_to_arrow_schema_by_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        } else {
            parquet_to_arrow_schema_by_root_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        }
    }

    fn get_record_reader(&mut self, batch_size: usize) -> Result<ParquetRecordBatchReader> {
        let column_indices = 0..self
            .file_reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();

        self.get_record_reader_by_columns(column_indices, batch_size)
    }

    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader>
    where
        T: IntoIterator<Item = usize>,
    {
        let array_reader = build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            self.get_schema()?,
            column_indices,
            self.file_reader.clone(),
        )?;

        ParquetRecordBatchReader::try_new(batch_size, array_reader)
    }
}

impl ParquetFileArrowReader {
    pub fn new(file_reader: Arc<dyn FileReader>) -> Self {
        Self { file_reader }
    }

    // Expose the reader metadata
    pub fn get_metadata(&mut self) -> ParquetMetaData {
        self.file_reader.metadata().clone()
    }
}

pub struct ParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    schema: Schema,
}

impl Iterator for ParquetRecordBatchReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.array_reader.next_batch(self.batch_size) {
            Err(error) => Some(Err(error.into())),
            Ok(array) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                    ArrowError::Other("Struct array reader should return struct array".to_string())
                });
                match struct_array {
                    Err(err) => Some(Err(err)),
                    Ok(e) => match RecordBatch::try_new(self.schema.clone(), e.values().to_vec()) {
                        Err(err) => Some(Err(err)),
                        Ok(record_batch) => {
                            if record_batch.num_rows() > 0 {
                                Some(Ok(record_batch))
                            } else {
                                None
                            }
                        }
                    },
                }
            }
        }
    }
}

impl RecordBatchReader for ParquetRecordBatchReader {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ParquetRecordBatchReader {
    pub fn try_new(batch_size: usize, array_reader: Box<dyn ArrayReader>) -> Result<Self> {
        // Check that array reader is struct array reader
        array_reader
            .as_any()
            .downcast_ref::<StructArrayReader>()
            .unwrap();

        let schema = match array_reader.get_data_type() {
            DataType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Ok(Self {
            batch_size,
            array_reader,
            schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use parquet::file::serialized_reader::SerializedFileReader;

    use crate::record_batch::RecordBatchReader;
    use std::fs::File;
    use std::sync::Arc;

    fn test_read(file_name: &str) -> Result<()> {
        let file = File::open(format!(
            "testing/parquet-testing/data/{}.parquet",
            file_name,
        ))
        .unwrap();

        let reader = SerializedFileReader::new(file).expect("Failed to create serialized reader");

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

        let mut record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        // Verify that the schema was correctly parsed
        let original_schema = arrow_reader.get_schema().unwrap().fields().clone();
        assert_eq!(original_schema, *record_batch_reader.schema().fields());

        while let Some(batch) = record_batch_reader.next() {
            batch?;
        }
        Ok(())
    }

    #[test]
    fn test_alltypes_plain() -> Result<()> {
        test_read("alltypes_plain")
    }

    #[test]
    fn test_alltypes_plain_snappy() -> Result<()> {
        test_read("alltypes_plain.snappy")
    }

    #[test]
    fn test_binary() -> Result<()> {
        test_read("binary")
    }

    #[test]
    fn test_nulls_snappy() -> Result<()> {
        test_read("nulls.snappy")
    }

    #[test]
    fn test_single_nan() -> Result<()> {
        test_read("single_nan")
    }

    #[test]
    fn test_fixed_length_decimal() -> Result<()> {
        test_read("fixed_length_decimal")
    }
}
