// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod client;
pub mod conn;
#[cfg(feature = "flight-sql")]
mod flight_sql;
mod params;
mod placeholder;
pub mod rest_api;

pub use client::Client;
pub use client::Connection;
pub use client::InsertCursor;
pub use client::LoadMethod;
pub use client::QueryCursor;
pub use client::RowORM;
pub use conn::ConnectionInfo;
pub use params::Param;
pub use params::Params;

// pub use for convenience
pub use databend_client::schema::{
    DataType, DecimalSize, Field, NumberDataType, Schema, SchemaRef,
};
pub use databend_driver_core::error::{Error, Result};
pub use databend_driver_core::rows::{
    Row, RowIterator, RowStatsIterator, RowWithStats, ServerStats,
};
pub use databend_driver_core::value::Interval;
pub use databend_driver_core::value::{NumberValue, Value};

pub use databend_driver_macros::serde_bend;
pub use databend_driver_macros::TryFromRow;

#[doc(hidden)]
pub mod _macro_internal {
    pub use databend_driver_core::_macro_internal::*;
}
