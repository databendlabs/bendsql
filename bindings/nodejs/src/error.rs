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

use databend_client::Error as ApiError;
use databend_driver::{Error as DriverError, Result as DriverResult};
use napi::{bindgen_prelude::*, Env, JsError};

/// Keep driver errors intact until napi-rs converts the result on the JS thread.
/// An outer `napi::Result` would reject before reaching this conversion.
pub struct DriverOutcome<T>(DriverResult<T>);

impl<T> From<DriverResult<T>> for DriverOutcome<T> {
    fn from(result: DriverResult<T>) -> Self {
        Self(result)
    }
}

impl<T: ToNapiValue> ToNapiValue for DriverOutcome<T> {
    unsafe fn to_napi_value(env: sys::napi_env, outcome: Self) -> Result<sys::napi_value> {
        match outcome.0 {
            Ok(value) => unsafe { T::to_napi_value(env, value) },
            // Returning Err here makes napi-rs reject with the original JS Error object.
            Err(error) => Err(js_error(Env::from(env), error)?),
        }
    }
}

fn js_error(env: Env, error: DriverError) -> Result<Error> {
    let legacy = Error::from_reason(error.to_string());
    let DriverError::Api(api_error) = &error else {
        return Ok(legacy);
    };

    // Start with the same Error object napi-rs would create for `legacy`,
    // including its existing message, name and string code (GenericFailure).
    let mut object = JsError::from(legacy).into_unknown(env).coerce_to_object()?;
    let mut inner = api_error;
    let (mut kind, mut id, mut retries) = (None, None, None);
    while let ApiError::WithContext {
        inner: next,
        request_kind,
        query_id,
        retry_times,
    } = inner
    {
        kind = kind.or(request_kind.as_ref());
        id = id.or(query_id.as_deref());
        retries = retries.or(*retry_times);
        inner = next;
    }

    if let Some(kind) = kind {
        object.set_named_property("requestKind", kind.to_string())?;
    }
    if let Some(id) = id {
        object.set_named_property("queryId", id)?;
    }
    if let Some(retries) = retries {
        object.set_named_property("retryTimes", retries)?;
    }
    if let ApiError::QueryFailed(code) | ApiError::AuthFailure(code) | ApiError::Logic(_, code) =
        inner
    {
        object.set_named_property("errorCode", u32::from(code.code))?;
    }
    // The REST client emits QueryFailed only for an HTTP 200 query response.
    // AuthFailure can come from either 200 or 401 and does not retain the status.
    if let Some(status) = match inner {
        ApiError::QueryFailed(_) => Some(200),
        _ => inner.status_code().map(|status| u32::from(status.as_u16())),
    } {
        object.set_named_property("httpStatus", status)?;
    }
    Ok(Error::from(object.into_unknown()))
}
