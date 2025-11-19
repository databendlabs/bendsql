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

use crate::error::Error;
use crate::error::Result;
use databend_client::GeometryDataType;
use geo::Geometry;
use geozero::geo_types::GeoWriter;
use geozero::wkb::Ewkb;
use geozero::{GeomProcessor, GeozeroGeometry, ToJson, ToWkb, ToWkt};

pub fn convert_geometry(raw_data: &[u8], typ: GeometryDataType) -> Result<String> {
    let (geo, srid) =
        ewkb_to_geo(&mut Ewkb(raw_data)).map_err(|e| Error::Parsing(e.to_string()))?;
    match typ {
        GeometryDataType::WKB => geo.to_wkb(geo.dims()).map(hex::encode_upper),
        GeometryDataType::WKT => geo.to_wkt(),
        GeometryDataType::EWKB => geo.to_ewkb(geo.dims(), srid).map(hex::encode_upper),
        GeometryDataType::EWKT => geo.to_ewkt(srid),
        GeometryDataType::GEOJSON => geo.to_json(),
    }
    .map_err(|e| Error::Parsing(e.to_string()))
}

// Process EWKB input and return Geometry object and SRID.
pub fn ewkb_to_geo<B: AsRef<[u8]>>(ewkb: &mut Ewkb<B>) -> Result<(Geometry<f64>, Option<i32>)> {
    let mut ewkb_processor = EwkbProcessor::new();
    ewkb.process_geom(&mut ewkb_processor)?;

    let geo = ewkb_processor
        .geo_writer
        .take_geometry()
        .ok_or_else(|| Error::Parsing("Invalid ewkb format".to_string()))?;
    let srid = ewkb_processor.srid;
    Ok((geo, srid))
}

struct EwkbProcessor {
    geo_writer: GeoWriter,
    srid: Option<i32>,
}

impl EwkbProcessor {
    fn new() -> Self {
        Self {
            geo_writer: GeoWriter::new(),
            srid: None,
        }
    }
}

impl GeomProcessor for EwkbProcessor {
    fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
        self.srid = srid;
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.xy(x, y, idx)
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.point_begin(idx)
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.point_end(idx)
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipoint_begin(size, idx)
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipoint_end(idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geo_writer.linestring_begin(tagged, size, idx)
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.linestring_end(tagged, idx)
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multilinestring_begin(size, idx)
    }

    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multilinestring_end(idx)
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geo_writer.polygon_begin(tagged, size, idx)
    }

    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.polygon_end(tagged, idx)
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipolygon_begin(size, idx)
    }

    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipolygon_end(idx)
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.geometrycollection_begin(size, idx)
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.geometrycollection_end(idx)
    }
}
