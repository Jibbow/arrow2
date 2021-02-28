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

use std::result::Result::Ok;
use std::sync::Arc;
use std::vec::Vec;
use std::{any::Any, cmp::min, iter::FromIterator};

use crate::array::*;
use crate::error::{ArrowError, Result};
use crate::{buffer::MutableBitmap, datatypes::DataType};

use super::ArrayReader;

type ArrayRef = Arc<dyn Array>;

/// Implementation of struct array reader.
pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: DataType,
    struct_def_level: i16,
    def_level_buffer: Option<Vec<i16>>,
    rep_level_buffer: Option<Vec<i16>>,
}

impl StructArrayReader {
    /// Construct struct array reader.
    pub fn new(data_type: DataType, children: Vec<Box<dyn ArrayReader>>, def_level: i16) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a struct.
    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Read `batch_size` struct records.
    ///
    /// Definition levels of struct array is calculated as following:
    /// ```ignore
    /// def_levels[i] = min(child1_def_levels[i], child2_def_levels[i], ...,
    /// childn_def_levels[i]);
    /// ```
    ///
    /// Repetition levels of struct array is calculated as following:
    /// ```ignore
    /// rep_levels[i] = child1_rep_levels[i];
    /// ```
    ///
    /// The null bitmap of struct array is calculated from def_levels:
    /// ```ignore
    /// null_bitmap[i] = (def_levels[i] >= self.def_level);
    /// ```
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        if self.children.is_empty() {
            self.def_level_buffer = None;
            self.rep_level_buffer = None;
            return Ok(Arc::new(StructArray::from_data(vec![], vec![], None)));
        }

        let children_array = self
            .children
            .iter_mut()
            .map(|reader| reader.next_batch(batch_size))
            .try_fold(
                Vec::new(),
                |mut result, child_array| -> Result<Vec<ArrayRef>> {
                    result.push(child_array?);
                    Ok(result)
                },
            )?;

        // check that array child data has same size
        let children_array_len = children_array.first().map(|arr| arr.len()).ok_or_else(|| {
            ArrowError::ExternalFormat(format!(
                "The struct has no childs but parquet requires structs to be non-empty."
            ))
        })?;

        let all_children_len_eq = children_array
            .iter()
            .all(|arr| arr.len() == children_array_len);
        if !all_children_len_eq {
            return Err(ArrowError::ExternalFormat(format!(
                "Not all children array length are the same!"
            )));
        }

        // calculate struct def level data
        let mut def_level_data = vec![self.struct_def_level; children_array_len];

        for child in &self.children {
            if let Some(current_child_def_levels) = child.get_def_levels() {
                if current_child_def_levels.len() != children_array_len {
                    return Err(ArrowError::ExternalFormat(format!(
                        "Child array length are not equal!"
                    )));
                } else {
                    for i in 0..children_array_len {
                        def_level_data[i] = min(def_level_data[i], current_child_def_levels[i]);
                    }
                }
            }
        }

        // calculate bitmap for current array
        let validity = MutableBitmap::from_iter(
            def_level_data
                .iter()
                .map(|def_level| *def_level >= self.struct_def_level),
        )
        .into();

        // calculate struct rep level data, since struct doesn't add to repetition
        // levels, here we just need to keep repetition levels of first array
        // TODO: Verify that all children array reader has same repetition levels
        let rep_level_data = self
            .children
            .first()
            .ok_or_else(|| {
                ArrowError::ExternalFormat(format!(
                    "Struct array reader should have at least one child!"
                ))
            })?
            .get_rep_levels()
            .map(|data| -> Result<Vec<i16>> { Ok(data.to_vec()) })
            .transpose()?;

        self.def_level_buffer = Some(def_level_data);
        self.rep_level_buffer = rep_level_data;
        let fields = StructArray::get_fields(&self.data_type).to_vec();
        Ok(Arc::new(StructArray::from_data(
            fields,
            children_array,
            validity,
        )))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer.as_ref().map(|x| x.as_ref())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer.as_ref().map(|x| x.as_ref())
    }
}
