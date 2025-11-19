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
use chrono_tz::Tz;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, Default, Copy)]
pub struct ResultFormatSettings {
    pub geometry_output_format: GeometryDataType,
    pub timezone: Tz,
}

impl ResultFormatSettings {
    pub fn from_map(settings: &Option<BTreeMap<String, String>>) -> Result<Self> {
        match settings {
            None => Ok(Default::default()),
            Some(settings) => {
                let timezone = match settings.get("timezone") {
                    None => Tz::default(),
                    Some(t) => Tz::from_str(t).map_err(|e| Error::Decode(e.to_string()))?,
                };

                let geometry_output_format = match settings.get("geometry_output_format") {
                    None => GeometryDataType::default(),
                    Some(t) => {
                        GeometryDataType::from_str(t).map_err(|e| Error::Decode(e.to_string()))?
                    }
                };

                Ok(Self {
                    timezone,
                    geometry_output_format,
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
