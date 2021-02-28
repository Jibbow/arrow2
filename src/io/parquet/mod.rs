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

//! This mod provides API for converting between arrow and parquet.
//!
//! # Example of reading parquet file into arrow record batch
//!
//! ```rust, no_run
//! use arrow::record_batch::RecordBatchReader;
//! use parquet::file::reader::SerializedFileReader;
//! use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
//! use std::sync::Arc;
//! use std::fs::File;
//!
//! let file = File::open("parquet.file").unwrap();
//! let file_reader = SerializedFileReader::new(file).unwrap();
//! let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
//!
//! println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());
//! println!("Arrow schema after projection is: {}",
//!    arrow_reader.get_schema_by_columns(vec![2, 4, 6], true).unwrap());
//!
//! let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();
//!
//! for maybe_record_batch in record_batch_reader {
//!    let record_batch = maybe_record_batch.unwrap();
//!    if record_batch.num_rows() > 0 {
//!        println!("Read {} records.", record_batch.num_rows());
//!    } else {
//!        println!("End of file!");
//!    }
//!}
//! ```

use parquet::errors::ParquetError;

use crate::error::ArrowError;

mod array_reader;
mod levels;
mod read;
mod record_reader;
mod schema;

pub mod arrow_reader;

// Schema metadata key used to store serialized Arrow IPC schema
pub const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

impl From<ParquetError> for ArrowError {
    fn from(error: ParquetError) -> Self {
        ArrowError::External("".to_string(), Box::new(error))
    }
}

impl Into<ParquetError> for ArrowError {
    fn into(self) -> ParquetError {
        ParquetError::General(format!("Arrow: {}", self))
    }
}
