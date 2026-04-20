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

use crate::error::Result;
use crate::Error;
use jiff::tz::TimeZone;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct ResultFormatSettings {
    pub geometry_output_format: GeometryDataType,
    pub timezone: TimeZone,
    pub arrow_result_version: Option<i64>,
    pub binary_output_format: BinaryFormat,
}

impl Default for ResultFormatSettings {
    fn default() -> Self {
        Self {
            geometry_output_format: GeometryDataType::default(),
            binary_output_format: BinaryFormat::default(),
            timezone: TimeZone::UTC,
            arrow_result_version: None,
        }
    }
}

impl ResultFormatSettings {
    pub fn from_map(settings: &Option<BTreeMap<String, String>>) -> Result<Self> {
        match settings {
            None => Ok(Default::default()),
            Some(settings) => {
                let timezone = match settings.get("timezone") {
                    None => TimeZone::UTC,
                    Some(t) => TimeZone::get(t).map_err(|e| Error::Decode(e.to_string()))?,
                };

                let geometry_output_format = match settings.get("geometry_output_format") {
                    None => GeometryDataType::default(),
                    Some(t) => {
                        GeometryDataType::from_str(t).map_err(|e| Error::Decode(e.to_string()))?
                    }
                };

                let binary_output_format = match settings.get("binary_output_format") {
                    None => BinaryFormat::default(),
                    Some(t) => {
                        BinaryFormat::from_str(t).map_err(|e| Error::Decode(e.to_string()))?
                    }
                };

                let arrow_result_version = match settings.get("arrow_result_version") {
                    None => None,
                    Some(t) => Some(t.parse().map_err(|_| {
                        Error::Decode(format!("invalid arrow result version '{t}"))
                    })?),
                };

                Ok(Self {
                    timezone,
                    arrow_result_version,
                    geometry_output_format,
                    binary_output_format,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub enum GeometryDataType {
    WKB,
    WKT,
    EWKB,
    EWKT,
    #[default]
    GEOJSON,
}

impl FromStr for GeometryDataType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "WKB" => Ok(GeometryDataType::WKB),
            "WKT" => Ok(GeometryDataType::WKT),
            "EWKB" => Ok(GeometryDataType::EWKB),
            "EWKT" => Ok(GeometryDataType::EWKT),
            "GEOJSON" => Ok(GeometryDataType::GEOJSON),
            _ => Err(Error::Decode("Invalid geometry type format".to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BinaryFormat {
    #[default]
    Hex,
    Base64,
    Utf8,
    Utf8Lossy,
}

impl FromStr for BinaryFormat {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hex" => Ok(BinaryFormat::Hex),
            "base64" => Ok(BinaryFormat::Base64),
            "utf-8" | "utf8" => Ok(BinaryFormat::Utf8),
            "utf-8-lossy" | "utf8-lossy" => Ok(BinaryFormat::Utf8Lossy),
            other => Err(Error::Decode(format!(
                "Invalid binary format '{other}', valid values: HEX | BASE64 | UTF-8 | UTF-8-LOSSY"
            ))),
        }
    }
}
