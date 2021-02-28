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

use std::convert::{From, TryInto};
use std::marker::PhantomData;
use std::sync::Arc;

use parquet::data_type::Int32Type as ParquetInt32Type;
use parquet::data_type::{ByteArray, DataType as ParquetType, FixedLenByteArray, Int96};

use crate::error::Result;
use crate::{
    array::*,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    types::{days_ms, NativeType},
};

/// A converter is used to consume record reader's content and convert it to arrow
/// primitive array.
pub trait Converter<S, T> {
    /// This method converts record reader's buffered content into arrow array.
    /// It will consume record reader's data, but will not reset record reader's
    /// state.
    fn convert(&self, source: S) -> Result<T>;
}

pub struct FixedSizeArrayConverter {
    byte_width: i32,
}

impl FixedSizeArrayConverter {
    pub fn new(byte_width: i32) -> Self {
        Self { byte_width }
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, FixedSizeBinaryArray> for FixedSizeArrayConverter {
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<FixedSizeBinaryArray> {
        let iter = source.iter().map(|x| Ok(x.as_ref().map(|x| x.data())));
        FixedSizeBinaryPrimitive::try_from_iter(iter)
            .map(|x| x.to(DataType::FixedSizeBinary(self.byte_width)))
            .map_err(|x| x.into())
    }
}

pub struct DecimalArrayConverter {
    precision: i32,
    scale: i32,
}

impl DecimalArrayConverter {
    pub fn new(precision: i32, scale: i32) -> Self {
        Self { precision, scale }
    }

    fn from_bytes_to_i128(b: &[u8]) -> i128 {
        assert!(b.len() <= 16, "DecimalArray supports only up to size 16");
        let first_bit = b[0] & 128u8 == 128u8;
        let mut result = if first_bit { [255u8; 16] } else { [0u8; 16] };
        for (i, v) in b.iter().enumerate() {
            result[i + (16 - b.len())] = *v;
        }
        i128::from_be_bytes(result)
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, PrimitiveArray<i128>> for DecimalArrayConverter {
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<PrimitiveArray<i128>> {
        let iter = source
            .into_iter()
            .map(|x| x.map(|x| Self::from_bytes_to_i128(x.data())));
        Ok(
            unsafe { Primitive::<i128>::from_trusted_len_iter(iter) }.to(DataType::Decimal(
                self.precision as usize,
                self.scale as usize,
            )),
        )
    }
}

/// An Arrow Interval converter, which reads the first 4 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow YearMonth value
pub struct IntervalYearMonthArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, PrimitiveArray<i32>>
    for IntervalYearMonthArrayConverter
{
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<PrimitiveArray<i32>> {
        let iter = source
            .into_iter()
            .map(|x| x.map(|x| i32::from_le_bytes(x.data()[0..4].try_into().unwrap())));
        Ok(unsafe { Primitive::<i32>::from_trusted_len_iter(iter) }
            .to(DataType::Interval(IntervalUnit::YearMonth)))
    }
}

/// An Arrow Interval converter, which reads the last 8 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow DayTime value
pub struct IntervalDayTimeArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, PrimitiveArray<days_ms>>
    for IntervalDayTimeArrayConverter
{
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<PrimitiveArray<days_ms>> {
        let iter = source
            .into_iter()
            .map(|x| x.map(|x| i64::from_le_bytes(x.data()[4..12].try_into().unwrap())));

        todo!()
        //Ok(unsafe { Primitive::<days_ms>::from_trusted_len_iter(iter) }
        //    .to(DataType::Interval(IntervalUnit::DayTime)))
    }
}

pub struct Int96ArrayConverter {
    pub timezone: Option<String>,
}

impl Converter<Vec<Option<Int96>>, PrimitiveArray<i64>> for Int96ArrayConverter {
    fn convert(&self, source: Vec<Option<Int96>>) -> Result<PrimitiveArray<i64>> {
        let iter = source
            .into_iter()
            .map(|int96| int96.map(|val| val.to_i64() * 1_000_000));

        Ok(
            unsafe { Primitive::<i64>::from_trusted_len_iter(iter) }.to(DataType::Timestamp(
                TimeUnit::Nanosecond,
                self.timezone.clone(),
            )),
        )
    }
}

pub struct Utf8ArrayConverter {}

impl<O: Offset> Converter<Vec<Option<ByteArray>>, Utf8Array<O>> for Utf8ArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<Utf8Array<O>> {
        let iter = source.iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_utf8().map_err(|e| e.into()))
                .transpose()
        });
        Utf8Primitive::<O>::try_from_iter(iter)
            .map(|x| x.to())
            .map_err(|x| x.into())
    }
}

pub struct BinaryArrayConverter {}

impl<O: Offset> Converter<Vec<Option<ByteArray>>, BinaryArray<O>> for BinaryArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<BinaryArray<O>> {
        let iter = source.iter().map(|x| Ok(x.as_ref().map(|x| x.data())));
        BinaryPrimitive::<O>::try_from_iter(iter)
            .map(|x| x.to())
            .map_err(|x| x.into())
    }
}

pub struct StringDictionaryArrayConverter {}

impl<K: DictionaryKey> Converter<Vec<Option<ByteArray>>, DictionaryArray<K>>
    for StringDictionaryArrayConverter
{
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<DictionaryArray<K>> {
        let iter = source.iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_utf8().map_err(|e| e.into()))
                .transpose()
        });

        DictionaryPrimitive::<K, Utf8Primitive<i32>, &str>::try_from_iter(iter)
            .map(|x| {
                x.to(DataType::Dictionary(
                    Box::new(K::DATA_TYPE),
                    Box::new(DataType::Utf8),
                ))
            })
            .map_err(|e| e.into())
    }
}

pub struct DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, ParquetType> {
    _dict_value_source_marker: PhantomData<DictValueSourceType>,
    _dict_value_target_marker: PhantomData<DictValueTargetType>,
    _parquet_marker: PhantomData<ParquetType>,
}

impl<DictValueSourceType, DictValueTargetType, ParquetType>
    DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, ParquetType>
{
    pub fn new() -> Self {
        Self {
            _dict_value_source_marker: PhantomData,
            _dict_value_target_marker: PhantomData,
            _parquet_marker: PhantomData,
        }
    }
}

impl<K, DictValueSourceType, DictValueTargetType, T>
    Converter<Vec<Option<<T as ParquetType>::T>>, DictionaryArray<K>>
    for DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, T>
where
    K: DictionaryKey,
    DictValueSourceType: NativeType,
    DictValueTargetType: NativeType,
    T: ParquetType,
    PrimitiveArray<DictValueSourceType>: From<Vec<Option<<T as ParquetType>::T>>>,
{
    fn convert(&self, source: Vec<Option<<T as ParquetType>::T>>) -> Result<DictionaryArray<K>> {
        todo!()
    }
}

pub type Utf8Converter<O> =
    ArrayRefConverter<Vec<Option<ByteArray>>, Utf8Array<O>, Utf8ArrayConverter>;
pub type BinaryConverter<O> =
    ArrayRefConverter<Vec<Option<ByteArray>>, BinaryArray<O>, BinaryArrayConverter>;

pub type StringDictionaryConverter<T> =
    ArrayRefConverter<Vec<Option<ByteArray>>, DictionaryArray<T>, StringDictionaryArrayConverter>;
pub type DictionaryConverter<K, SV, TV, P> = ArrayRefConverter<
    Vec<Option<<P as ParquetType>::T>>,
    DictionaryArray<K>,
    DictionaryArrayConverter<SV, TV, P>,
>;
pub type PrimitiveDictionaryConverter<K, V> = ArrayRefConverter<
    Vec<Option<<ParquetInt32Type as ParquetType>::T>>,
    DictionaryArray<K>,
    DictionaryArrayConverter<i32, V, ParquetInt32Type>,
>;

pub type Int96Converter =
    ArrayRefConverter<Vec<Option<Int96>>, PrimitiveArray<i64>, Int96ArrayConverter>;

pub type FixedLenBinaryConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    FixedSizeBinaryArray,
    FixedSizeArrayConverter,
>;
pub type IntervalYearMonthConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    PrimitiveArray<i32>,
    IntervalYearMonthArrayConverter,
>;
pub type IntervalDayTimeConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    PrimitiveArray<i64>,
    IntervalDayTimeArrayConverter,
>;

pub type DecimalConverter =
    ArrayRefConverter<Vec<Option<FixedLenByteArray>>, PrimitiveArray<i128>, DecimalArrayConverter>;

pub struct FromConverter<S, T> {
    _source: PhantomData<S>,
    _dest: PhantomData<T>,
}

impl<S, T> FromConverter<S, T>
where
    T: From<S>,
{
    pub fn new() -> Self {
        Self {
            _source: PhantomData,
            _dest: PhantomData,
        }
    }
}

impl<S, T> Converter<S, T> for FromConverter<S, T>
where
    T: From<S>,
{
    fn convert(&self, source: S) -> Result<T> {
        Ok(T::from(source))
    }
}

type ArrayRef = Arc<dyn Array>;

pub struct ArrayRefConverter<S, A, C> {
    _source: PhantomData<S>,
    _array: PhantomData<A>,
    converter: C,
}

impl<S, A, C> ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    pub fn new(converter: C) -> Self {
        Self {
            _source: PhantomData,
            _array: PhantomData,
            converter,
        }
    }
}

impl<S, A, C> Converter<S, ArrayRef> for ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    fn convert(&self, source: S) -> Result<ArrayRef> {
        self.converter
            .convert(source)
            .map(|array| Arc::new(array) as ArrayRef)
    }
}
