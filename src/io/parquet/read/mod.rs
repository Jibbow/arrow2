use std::any::Any;
use std::sync::Arc;

use crate::array::Array;
use crate::datatypes::DataType;
use crate::error::Result;

mod boolean;
pub mod converter;
mod int96;
mod null;
mod primitive;
mod structure;
mod variable_size;

pub use boolean::BooleanArrayReader;
pub use int96::DeprecatedInt96Timestamp;
pub use null::NullArrayReader;
pub use primitive::PrimitiveArrayReader;
pub use structure::StructArrayReader;
pub use variable_size::VariableSizeArrayReader;

/// Array reader reads parquet data into arrow array.
pub trait ArrayReader {
    fn as_any(&self) -> &dyn Any;

    /// Returns the arrow type of this array reader.
    fn get_data_type(&self) -> &DataType;

    /// Reads at most `batch_size` records into an arrow array and return it.
    fn next_batch(&mut self, batch_size: usize) -> Result<Arc<dyn Array>>;

    /// Returns the definition levels of data from last call of `next_batch`.
    /// The result is used by parent array reader to calculate its own definition
    /// levels and repetition levels, so that its parent can calculate null bitmap.
    fn get_def_levels(&self) -> Option<&[i16]>;

    /// Return the repetition levels of data from last call of `next_batch`.
    /// The result is used by parent array reader to calculate its own definition
    /// levels and repetition levels, so that its parent can calculate null bitmap.
    fn get_rep_levels(&self) -> Option<&[i16]>;
}
