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

use databend_driver_core::error::Error as CoreError;
use http::StatusCode;
use pyo3::{create_exception, exceptions::PyException, prelude::*};

// PEP-249 compliant exception hierarchy

// Base exceptions
create_exception!(databend_driver, Warning, PyException);
create_exception!(databend_driver, Error, PyException);

// Interface errors
create_exception!(databend_driver, InterfaceError, Error);

// Database errors
create_exception!(databend_driver, DatabaseError, Error);

// Specific database error types
create_exception!(databend_driver, DataError, DatabaseError);
create_exception!(databend_driver, OperationalError, DatabaseError);
create_exception!(databend_driver, IntegrityError, DatabaseError);
create_exception!(databend_driver, InternalError, DatabaseError);
create_exception!(databend_driver, ProgrammingError, DatabaseError);
create_exception!(databend_driver, NotSupportedError, DatabaseError);

/// Map error code to appropriate PEP-249 exception based on actual Databend error code definitions
///
/// This mapping is based on real error code definitions from Databend source code
/// (src/common/exception/src/exception_code.rs) and categorized according to error
/// nature and PEP-249 standards
fn map_error_code_to_exception(error_code: u16, error_msg: String) -> PyErr {
    match error_code {
        // Core System Errors [0-1000] - Internal system errors
        0 => DatabaseError::new_err(error_msg),        // Ok
        1001 => InternalError::new_err(error_msg),     // Internal
        1002 => NotSupportedError::new_err(error_msg), // Unimplemented

        // Database and Table Access Errors [1003-1004, 1020, 1025-1026, 1058, 1119-1120, 2318-2320] - Programming errors
        1003 | 1004 | 1020 | 1025 | 1026 | 1058 | 1119 | 1120 | 2318 | 2319 | 2320 => {
            ProgrammingError::new_err(error_msg) // Database/Table/Column/Catalog not found or already exists
        }

        // Syntax and Semantic Errors [1005-1010, 1027-1028, 1065] - Programming errors
        1005 | 1006 | 1007 | 1008 | 1010 | 1027 | 1028 | 1065 => {
            ProgrammingError::new_err(error_msg) // Syntax/Semantic errors, bad arguments
        }

        // Data Structure Errors [1016-1018, 1030, 1114] - Data errors
        1016 | 1017 | 1018 | 1030 | 1114 => {
            DataError::new_err(error_msg) // Data structure mismatches, empty data
        }

        // Network and Communication Errors [1036-1038] - Operational errors
        1036..=1038 => {
            OperationalError::new_err(error_msg) // Network/connectivity issues
        }

        // Session and Query Errors [1041-1044, 1053, 1127] - Operational errors
        1041 | 1042 | 1043 | 1044 | 1053 | 1127 => {
            OperationalError::new_err(error_msg) // Connection/session/query management issues
        }

        // Internal System Errors - Internal errors
        1047 | 1049 | 1104 | 1122 | 1123 | 1067 | 1068 => {
            InternalError::new_err(error_msg) // Prometheus, overflow, panic, timeout, runtime errors
        }

        // Permission and Security Errors [1052, 1061-1063, 1066, 2506] - Operational errors
        1052 | 1061 | 1062 | 1063 | 1066 | 2506 => {
            OperationalError::new_err(error_msg) // TLS, permission, authentication issues
        }

        // Data Format and Parsing Errors [1046, 1057, 1060, 1064, 1072, 1074-1081, 1090, 1201-1202, 2507-2509] - Data errors
        1046 | 1057 | 1060 | 1064 | 1072 | 1074..=1081 | 1090 | 1201 | 1202 | 2507..=2509 => {
            DataError::new_err(error_msg) // Parsing, format, compression, date/time errors
        }

        // Table Structure and Operation Errors [1102-1103, 1106-1118, 1121, 1130-1132] - Programming errors
        1102 | 1103 | 1106..=1118 | 1121 | 1130..=1132 => {
            ProgrammingError::new_err(error_msg) // Table schema, column operations
        }

        // Sequence Errors [1124-1126, 3101] - Data errors
        1124..=1126 | 3101 => {
            DataError::new_err(error_msg) // Sequence range/count issues
        }

        // Virtual Column Errors [1128-1129] - Programming errors
        1128 | 1129 => {
            ProgrammingError::new_err(error_msg) // Virtual column configuration
        }

        // Table Engine Errors [1301-1303, 2701-2703] - Not supported errors
        1301..=1303 | 2701..=2703 => {
            NotSupportedError::new_err(error_msg) // Engine not supported
        }

        // License Errors [1401-1404] - Operational errors
        1401..=1404 => {
            OperationalError::new_err(error_msg) // License key issues
        }

        // Index Errors [1503, 1601-1603, 2720-2726] - Programming errors (1111 already covered above)
        1503 | 1601..=1603 | 2720..=2726 => {
            ProgrammingError::new_err(error_msg) // Index operations
        }

        // Cloud and Integration Errors [1701-1703] - Operational errors
        1701..=1703 => {
            OperationalError::new_err(error_msg) // Cloud control connectivity
        }

        // UDF and Extension Errors [1810, 2601-2607] - Programming and data errors
        1810 | 2601..=2603 | 2605 => ProgrammingError::new_err(error_msg), // UDF format, schema errors
        2604 => OperationalError::new_err(error_msg),                      // UDF server connection
        2606 | 2607 => DataError::new_err(error_msg),                      // Data type, data errors

        // Task Errors [2611-2616] - Programming errors
        2611..=2616 => {
            ProgrammingError::new_err(error_msg) // Task configuration errors
        }

        // Search and External Service Errors [1901-1903, 1910] - Operational errors
        1901..=1903 | 1910 => {
            OperationalError::new_err(error_msg) // Search service, HTTP request errors
        }

        // Meta Service Core Errors [2001-2016] - Internal errors
        2001..=2016 => {
            InternalError::new_err(error_msg) // Meta service internal issues
        }

        // User and Role Management Errors [2201-2218] - Programming errors
        2201..=2218 => {
            ProgrammingError::new_err(error_msg) // User/role configuration issues
        }

        // Database and Catalog Management Errors [2301-2317, 2321-2324] - Programming and integrity errors
        2301 | 2302 | 2306..=2317 | 2321 | 2324 => {
            ProgrammingError::new_err(error_msg) // Object already exists, drop/create operations
        }
        2322 | 2323 => IntegrityError::new_err(error_msg), // Commit/transaction issues

        // Stage and Connection Errors [2501-2505, 2510-2512] - Programming errors
        2501..=2505 | 2510..=2512 => {
            ProgrammingError::new_err(error_msg) // Stage/connection configuration
        }

        // Stream and Dynamic Table Errors [2730-2735, 2740] - Programming errors
        2730..=2735 | 2740 => {
            ProgrammingError::new_err(error_msg) // Stream configuration
        }

        // Sharing and Collaboration Errors [2705-2719, 3111-3112] - Programming errors
        2705..=2719 | 3111 | 3112 => {
            ProgrammingError::new_err(error_msg) // Share configuration
        }

        // Variable and Configuration Errors [2801-2803] - Programming errors
        2801..=2803 => {
            ProgrammingError::new_err(error_msg) // Variable configuration
        }

        // Tenant and Quota Errors [2901-2903] - Operational errors
        2901..=2903 => {
            OperationalError::new_err(error_msg) // Quota exceeded, tenant issues
        }

        // Script and Procedure Errors [3128-3132] - Programming errors
        3128..=3132 => {
            ProgrammingError::new_err(error_msg) // Script/procedure issues
        }

        // Storage and I/O Errors [3001-3002, 3901-3905, 4000] - Operational errors
        3001 | 3002 | 3901..=3905 | 4000 => {
            OperationalError::new_err(error_msg) // Storage access issues
        }

        // Dictionary Errors [3113-3115] - Programming errors
        3113..=3115 => {
            ProgrammingError::new_err(error_msg) // Dictionary configuration
        }

        // Workload Management Errors [3140-3144] - Programming errors
        3140..=3144 => {
            ProgrammingError::new_err(error_msg) // Workload configuration
        }

        // Transaction and Processing Errors [4001-4004, 4012-4013] - Operational errors
        4001..=4004 | 4012 | 4013 => {
            OperationalError::new_err(error_msg) // Transaction conflicts, timeouts
        }

        // Service Status Errors [5002] - Operational errors
        5002 => {
            OperationalError::new_err(error_msg) // Service already stopped
        }

        // Authentication Errors [5100-5104] - Operational errors
        5100..=5104 => {
            OperationalError::new_err(error_msg) // Authentication, token issues
        }

        // Client Session Errors [5110-5115] - Operational errors
        5110..=5115 => {
            OperationalError::new_err(error_msg) // Session timeout, state issues
        }

        // Default case for unknown error codes
        _ => DatabaseError::new_err(error_msg),
    }
}

/// Map Databend driver errors to PEP-249 compliant exceptions based on the actual enum variants
pub fn map_error_to_exception(error: CoreError) -> PyErr {
    let error_msg = error.to_string();

    match error {
        // Parsing errors - syntax, invalid SQL, etc.
        CoreError::Parsing(_) => ProgrammingError::new_err(error_msg),

        // Protocol errors - communication issues with server
        CoreError::Protocol(_) => OperationalError::new_err(error_msg),

        // Transport errors - network connectivity issues
        CoreError::Transport(_) => OperationalError::new_err(error_msg),

        // IO errors - file system operations, etc.
        CoreError::IO(_) => OperationalError::new_err(error_msg),

        // Bad argument errors - client-side programming errors
        CoreError::BadArgument(_) => ProgrammingError::new_err(error_msg),

        // Invalid response errors - data format issues
        CoreError::InvalidResponse(_) => DataError::new_err(error_msg),

        // API errors - delegate to the API error type
        CoreError::Api(api_error) => map_api_error_to_exception(&api_error, error_msg),

        // Convert errors - data type conversion failures
        CoreError::Convert(_) => DataError::new_err(error_msg),

        // Arrow errors - data processing issues
        CoreError::Arrow(_) => DataError::new_err(error_msg),
    }
}

/// Map databend_client::Error to PEP-249 exceptions
fn map_api_error_to_exception(api_error: &databend_client::Error, error_msg: String) -> PyErr {
    match api_error {
        // BadArgument errors are programming errors
        databend_client::Error::BadArgument(_) => ProgrammingError::new_err(error_msg),

        // IO errors are operational issues
        databend_client::Error::IO(_) => OperationalError::new_err(error_msg),

        // Request errors are operational issues (network, connectivity)
        databend_client::Error::Request(_) => OperationalError::new_err(error_msg),

        // Decode errors are typically data-related
        databend_client::Error::Decode(_) => DataError::new_err(error_msg),

        // Query execution failures - categorize by error code
        databend_client::Error::QueryFailed(error_code) => {
            map_error_code_to_exception(error_code.code, error_msg)
        }

        // Logic errors with status codes
        databend_client::Error::Logic(status, error_code) => {
            match status {
                // Authentication/Authorization errors
                &StatusCode::UNAUTHORIZED | &StatusCode::FORBIDDEN => {
                    OperationalError::new_err(error_msg)
                }
                // Bad request - typically programming errors
                &StatusCode::BAD_REQUEST => ProgrammingError::new_err(error_msg),
                // Not found errors
                &StatusCode::NOT_FOUND => ProgrammingError::new_err(error_msg),
                // Method not allowed - not supported operations
                &StatusCode::METHOD_NOT_ALLOWED => NotSupportedError::new_err(error_msg),
                // Unprocessable entity - data errors
                &StatusCode::UNPROCESSABLE_ENTITY => DataError::new_err(error_msg),
                // Server errors
                status if status.is_server_error() => InternalError::new_err(error_msg),
                // For other status codes, use error code for categorization
                _ => map_error_code_to_exception(error_code.code, error_msg),
            }
        }

        // HTTP response errors
        databend_client::Error::Response { status, .. } => match status {
            // Authentication errors
            &StatusCode::UNAUTHORIZED | &StatusCode::FORBIDDEN => {
                OperationalError::new_err(error_msg)
            }
            // Client errors are typically programming issues
            status if status.is_client_error() => ProgrammingError::new_err(error_msg),
            // Server errors are internal
            status if status.is_server_error() => InternalError::new_err(error_msg),
            // Default to operational error
            _ => OperationalError::new_err(error_msg),
        },

        // Query not found - session expired, connection issues
        databend_client::Error::QueryNotFound(_) => OperationalError::new_err(error_msg),

        // Authentication failures
        databend_client::Error::AuthFailure(_) => OperationalError::new_err(error_msg),

        // Wrapped errors - unwrap and recurse
        databend_client::Error::WithContext(inner_error, context) => {
            let inner_err = map_api_error_to_exception(inner_error, inner_error.to_string());
            let context_msg = format!("{}: {}", context, inner_err);

            // Return a DatabaseError with context message to simplify the logic
            // The original error classification is preserved in the context
            DatabaseError::new_err(context_msg)
        }
    }
}

/// Register all exceptions with the Python module
pub fn register_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("Warning", py.get_type::<Warning>())?;
    m.add("Error", py.get_type::<Error>())?;
    m.add("InterfaceError", py.get_type::<InterfaceError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("DataError", py.get_type::<DataError>())?;
    m.add("OperationalError", py.get_type::<OperationalError>())?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
    m.add("ProgrammingError", py.get_type::<ProgrammingError>())?;
    m.add("NotSupportedError", py.get_type::<NotSupportedError>())?;
    Ok(())
}
