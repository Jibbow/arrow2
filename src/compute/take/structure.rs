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

use std::sync::Arc;

use crate::{
    array::{Array, Offset, PrimitiveArray, StructArray},
    bitmap::{Bitmap, MutableBitmap},
    error::Result,
};

use super::maybe_usize;

#[inline]
fn take_validity<I: Offset>(
    validity: &Option<Bitmap>,
    indices: &PrimitiveArray<I>,
) -> Result<Option<Bitmap>> {
    let indices_validity = indices.validity();
    match (validity, indices_validity) {
        (None, _) => Ok(indices_validity.clone()),
        (Some(validity), None) => {
            let iter = indices.values().iter().map(|x| {
                let index = maybe_usize(*x)?;
                Result::Ok(validity.get_bit(index))
            });
            Ok(unsafe { MutableBitmap::try_from_trusted_len_iter(iter) }?.into())
        }
        (Some(validity), _) => {
            let iter = indices.iter().map(|x| {
                Result::Ok(match x {
                    Some(x) => {
                        let index = maybe_usize(*x)?;
                        validity.get_bit(index)
                    }
                    None => false,
                })
            });
            Ok(unsafe { MutableBitmap::try_from_trusted_len_iter(iter) }?.into())
        }
    }
}

pub fn take<I: Offset>(array: &StructArray, indices: &PrimitiveArray<I>) -> Result<StructArray> {
    let values: Vec<Arc<dyn Array>> = array
        .values()
        .iter()
        .map(|a| super::take(a.as_ref(), indices).map(|x| x.into()))
        .collect::<Result<_>>()?;
    let validity = take_validity(array.validity(), indices)?;
    Ok(StructArray::from_data(
        array.fields().to_vec(),
        values,
        validity,
    ))
}
