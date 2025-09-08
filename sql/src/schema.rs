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

use std::sync::Arc;

use databend_client::SchemaField as APISchemaField;

use crate::error::{Error, Result};

#[cfg(feature = "flight-sql")]
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, SchemaRef as ArrowSchemaRef};

// Extension types defined by Databend
#[cfg(feature = "flight-sql")]
pub(crate) const EXTENSION_KEY: &str = "Extension";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_EMPTY_ARRAY: &str = "EmptyArray";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_EMPTY_MAP: &str = "EmptyMap";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_VARIANT: &str = "Variant";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_BITMAP: &str = "Bitmap";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_GEOMETRY: &str = "Geometry";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_GEOGRAPHY: &str = "Geography";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_INTERVAL: &str = "Interval";
#[cfg(feature = "flight-sql")]
pub(crate) const ARROW_EXT_TYPE_VECTOR: &str = "Vector";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumberDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalSize {
    pub precision: u8,
    pub scale: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalDataType {
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

impl DecimalDataType {
    pub fn decimal_size(&self) -> &DecimalSize {
        match self {
            DecimalDataType::Decimal128(size) => size,
            DecimalDataType::Decimal256(size) => size,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    Binary,
    String,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<DataType>),
    Array(Box<DataType>),
    Map(Box<DataType>),
    Tuple(Vec<DataType>),
    Variant,
    Bitmap,
    Geometry,
    Geography,
    Interval,
    Vector(u64),
    // Generic(usize),
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        match self {
            DataType::Number(_) | DataType::Decimal(_) => true,
            DataType::Nullable(inner) => inner.is_numeric(),
            _ => false,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "Null"),
            DataType::EmptyArray => write!(f, "EmptyArray"),
            DataType::EmptyMap => write!(f, "EmptyMap"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Binary => write!(f, "Binary"),
            DataType::String => write!(f, "String"),
            DataType::Number(n) => match n {
                NumberDataType::UInt8 => write!(f, "UInt8"),
                NumberDataType::UInt16 => write!(f, "UInt16"),
                NumberDataType::UInt32 => write!(f, "UInt32"),
                NumberDataType::UInt64 => write!(f, "UInt64"),
                NumberDataType::Int8 => write!(f, "Int8"),
                NumberDataType::Int16 => write!(f, "Int16"),
                NumberDataType::Int32 => write!(f, "Int32"),
                NumberDataType::Int64 => write!(f, "Int64"),
                NumberDataType::Float32 => write!(f, "Float32"),
                NumberDataType::Float64 => write!(f, "Float64"),
            },
            DataType::Decimal(d) => {
                let size = d.decimal_size();
                write!(f, "Decimal({}, {})", size.precision, size.scale)
            }
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Date => write!(f, "Date"),
            DataType::Nullable(inner) => write!(f, "Nullable({inner})"),
            DataType::Array(inner) => write!(f, "Array({inner})"),
            DataType::Map(inner) => match inner.as_ref() {
                DataType::Tuple(tys) => {
                    write!(f, "Map({}, {})", tys[0], tys[1])
                }
                _ => unreachable!(),
            },
            DataType::Tuple(inner) => {
                let inner = inner
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Tuple({inner})")
            }
            DataType::Variant => write!(f, "Variant"),
            DataType::Bitmap => write!(f, "Bitmap"),
            DataType::Geometry => write!(f, "Geometry"),
            DataType::Geography => write!(f, "Geography"),
            DataType::Interval => write!(f, "Interval"),
            DataType::Vector(d) => write!(f, "Vector({d})"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Default)]
pub struct Schema(Vec<Field>);

pub type SchemaRef = Arc<Schema>;

impl Schema {
    pub fn fields(&self) -> &[Field] {
        &self.0
    }

    pub fn from_vec(fields: Vec<Field>) -> Self {
        Self(fields)
    }
}

impl TryFrom<&TypeDesc<'_>> for DataType {
    type Error = Error;

    fn try_from(desc: &TypeDesc) -> Result<Self> {
        if desc.nullable {
            let mut desc = desc.clone();
            desc.nullable = false;
            let inner = DataType::try_from(&desc)?;
            return Ok(DataType::Nullable(Box::new(inner)));
        }
        let dt = match desc.name {
            "NULL" | "Null" => DataType::Null,
            "Boolean" => DataType::Boolean,
            "Binary" => DataType::Binary,
            "String" => DataType::String,
            "Int8" => DataType::Number(NumberDataType::Int8),
            "Int16" => DataType::Number(NumberDataType::Int16),
            "Int32" => DataType::Number(NumberDataType::Int32),
            "Int64" => DataType::Number(NumberDataType::Int64),
            "UInt8" => DataType::Number(NumberDataType::UInt8),
            "UInt16" => DataType::Number(NumberDataType::UInt16),
            "UInt32" => DataType::Number(NumberDataType::UInt32),
            "UInt64" => DataType::Number(NumberDataType::UInt64),
            "Float32" => DataType::Number(NumberDataType::Float32),
            "Float64" => DataType::Number(NumberDataType::Float64),
            "Decimal" => {
                let precision = desc.args[0].name.parse::<u8>()?;
                let scale = desc.args[1].name.parse::<u8>()?;

                if precision <= 38 {
                    DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                        precision,
                        scale,
                    }))
                } else {
                    DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                        precision,
                        scale,
                    }))
                }
            }
            "Timestamp" => DataType::Timestamp,
            "Date" => DataType::Date,
            "Nullable" => {
                if desc.args.len() != 1 {
                    return Err(Error::Parsing(
                        "Nullable type must have one argument".to_string(),
                    ));
                }
                let mut desc = desc.clone();
                // ignore inner NULL indicator
                desc.nullable = false;
                let inner = Self::try_from(&desc.args[0])?;
                DataType::Nullable(Box::new(inner))
            }
            "Array" => {
                if desc.args.len() != 1 {
                    return Err(Error::Parsing(
                        "Array type must have one argument".to_string(),
                    ));
                }
                if desc.args[0].name == "Nothing" {
                    DataType::EmptyArray
                } else {
                    let inner = Self::try_from(&desc.args[0])?;
                    DataType::Array(Box::new(inner))
                }
            }
            "Map" => {
                if desc.args.len() == 1 && desc.args[0].name == "Nothing" {
                    DataType::EmptyMap
                } else {
                    if desc.args.len() != 2 {
                        return Err(Error::Parsing(
                            "Map type must have two arguments".to_string(),
                        ));
                    }
                    let key_ty = Self::try_from(&desc.args[0])?;
                    let val_ty = Self::try_from(&desc.args[1])?;
                    DataType::Map(Box::new(DataType::Tuple(vec![key_ty, val_ty])))
                }
            }
            "Tuple" => {
                let mut inner = vec![];
                for arg in &desc.args {
                    inner.push(Self::try_from(arg)?);
                }
                DataType::Tuple(inner)
            }
            "Variant" => DataType::Variant,
            "Bitmap" => DataType::Bitmap,
            "Geometry" => DataType::Geometry,
            "Geography" => DataType::Geography,
            "Interval" => DataType::Interval,
            "Vector" => {
                let dimension = desc.args[0].name.parse::<u64>()?;
                DataType::Vector(dimension)
            }
            _ => return Err(Error::Parsing(format!("Unknown type: {desc:?}"))),
        };
        Ok(dt)
    }
}

impl TryFrom<APISchemaField> for Field {
    type Error = Error;

    fn try_from(f: APISchemaField) -> Result<Self> {
        let type_desc = parse_type_desc(&f.data_type)?;
        let dt = DataType::try_from(&type_desc)?;
        let field = Self {
            name: f.name,
            data_type: dt,
        };
        Ok(field)
    }
}

impl TryFrom<Vec<APISchemaField>> for Schema {
    type Error = Error;

    fn try_from(fields: Vec<APISchemaField>) -> Result<Self> {
        let fields = fields
            .into_iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(fields))
    }
}

#[cfg(feature = "flight-sql")]
impl TryFrom<&Arc<ArrowField>> for Field {
    type Error = Error;

    fn try_from(f: &Arc<ArrowField>) -> Result<Self> {
        let mut dt = if let Some(extend_type) = f.metadata().get(EXTENSION_KEY) {
            match extend_type.as_str() {
                ARROW_EXT_TYPE_EMPTY_ARRAY => DataType::EmptyArray,
                ARROW_EXT_TYPE_EMPTY_MAP => DataType::EmptyMap,
                ARROW_EXT_TYPE_VARIANT => DataType::Variant,
                ARROW_EXT_TYPE_BITMAP => DataType::Bitmap,
                ARROW_EXT_TYPE_GEOMETRY => DataType::Geometry,
                ARROW_EXT_TYPE_GEOGRAPHY => DataType::Geography,
                ARROW_EXT_TYPE_INTERVAL => DataType::Interval,
                ARROW_EXT_TYPE_VECTOR => match f.data_type() {
                    ArrowDataType::FixedSizeList(field, dimension) => {
                        let dimension = match field.data_type() {
                            ArrowDataType::Float32 => *dimension as u64,
                            _ => {
                                return Err(Error::Parsing(format!(
                                    "Unsupported FixedSizeList Arrow type: {:?}",
                                    field.data_type()
                                )));
                            }
                        };
                        DataType::Vector(dimension)
                    }
                    arrow_type => {
                        return Err(Error::Parsing(format!(
                            "Unsupported Arrow type: {arrow_type:?}",
                        )));
                    }
                },
                _ => {
                    return Err(Error::Parsing(format!(
                        "Unsupported extension datatype for arrow field: {f:?}"
                    )))
                }
            }
        } else {
            match f.data_type() {
                ArrowDataType::Null => DataType::Null,
                ArrowDataType::Boolean => DataType::Boolean,
                ArrowDataType::Int8 => DataType::Number(NumberDataType::Int8),
                ArrowDataType::Int16 => DataType::Number(NumberDataType::Int16),
                ArrowDataType::Int32 => DataType::Number(NumberDataType::Int32),
                ArrowDataType::Int64 => DataType::Number(NumberDataType::Int64),
                ArrowDataType::UInt8 => DataType::Number(NumberDataType::UInt8),
                ArrowDataType::UInt16 => DataType::Number(NumberDataType::UInt16),
                ArrowDataType::UInt32 => DataType::Number(NumberDataType::UInt32),
                ArrowDataType::UInt64 => DataType::Number(NumberDataType::UInt64),
                ArrowDataType::Float32 => DataType::Number(NumberDataType::Float32),
                ArrowDataType::Float64 => DataType::Number(NumberDataType::Float64),
                ArrowDataType::Binary
                | ArrowDataType::LargeBinary
                | ArrowDataType::FixedSizeBinary(_) => DataType::Binary,
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                    DataType::String
                }
                ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
                ArrowDataType::Date32 => DataType::Date,
                ArrowDataType::Decimal128(p, s) => {
                    DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                        precision: *p,
                        scale: *s as u8,
                    }))
                }
                ArrowDataType::Decimal256(p, s) => {
                    DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                        precision: *p,
                        scale: *s as u8,
                    }))
                }
                ArrowDataType::List(f) | ArrowDataType::LargeList(f) => {
                    let inner_field = Field::try_from(f)?;
                    let inner_ty = inner_field.data_type;
                    DataType::Array(Box::new(inner_ty))
                }
                ArrowDataType::Map(f, _) => {
                    let inner_field = Field::try_from(f)?;
                    let inner_ty = inner_field.data_type;
                    DataType::Map(Box::new(inner_ty))
                }
                ArrowDataType::Struct(fs) => {
                    let mut inner_tys = Vec::with_capacity(fs.len());
                    for f in fs {
                        let inner_field = Field::try_from(f)?;
                        let inner_ty = inner_field.data_type;
                        inner_tys.push(inner_ty);
                    }
                    DataType::Tuple(inner_tys)
                }
                _ => {
                    return Err(Error::Parsing(format!(
                        "Unsupported datatype for arrow field: {f:?}"
                    )))
                }
            }
        };
        if f.is_nullable() && !matches!(dt, DataType::Null) {
            dt = DataType::Nullable(Box::new(dt));
        }
        Ok(Field {
            name: f.name().to_string(),
            data_type: dt,
        })
    }
}

#[cfg(feature = "flight-sql")]
impl TryFrom<ArrowSchemaRef> for Schema {
    type Error = Error;

    fn try_from(schema_ref: ArrowSchemaRef) -> Result<Self> {
        let fields = schema_ref
            .fields()
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(fields))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TypeDesc<'t> {
    name: &'t str,
    nullable: bool,
    args: Vec<TypeDesc<'t>>,
}

fn parse_type_desc(s: &str) -> Result<TypeDesc<'_>> {
    let mut name = "";
    let mut args = vec![];
    let mut depth = 0;
    let mut start = 0;
    let mut nullable = false;
    for (i, c) in s.char_indices() {
        match c {
            '(' => {
                if depth == 0 {
                    name = &s[start..i];
                    start = i + 1;
                }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let s = &s[start..i];
                    if !s.is_empty() {
                        args.push(parse_type_desc(s)?);
                    }
                    start = i + 1;
                }
            }
            ',' => {
                if depth == 1 {
                    let s = &s[start..i];
                    args.push(parse_type_desc(s)?);
                    start = i + 1;
                }
            }
            ' ' => {
                if depth == 0 {
                    let s = &s[start..i];
                    if !s.is_empty() {
                        name = s;
                    }
                    start = i + 1;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Error::Parsing(format!("Invalid type desc: {s}")));
    }
    if start < s.len() {
        let s = &s[start..];
        if !s.is_empty() {
            if name.is_empty() {
                name = s;
            } else if s == "NULL" {
                nullable = true;
            } else {
                return Err(Error::Parsing(format!("Invalid type arg for {name}: {s}")));
            }
        }
    }
    Ok(TypeDesc {
        name,
        nullable,
        args,
    })
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    #[test]
    fn test_parse_type_desc() {
        struct TestCase<'t> {
            desc: &'t str,
            input: &'t str,
            output: TypeDesc<'t>,
        }
        let test_cases = vec![
            TestCase {
                desc: "plain type",
                input: "String",
                output: TypeDesc {
                    name: "String",
                    nullable: false,
                    args: vec![],
                },
            },
            TestCase {
                desc: "decimal type",
                input: "Decimal(42, 42)",
                output: TypeDesc {
                    name: "Decimal",
                    nullable: false,
                    args: vec![
                        TypeDesc {
                            name: "42",
                            nullable: false,
                            args: vec![],
                        },
                        TypeDesc {
                            name: "42",
                            nullable: false,
                            args: vec![],
                        },
                    ],
                },
            },
            TestCase {
                desc: "nullable type",
                input: "Nullable(Nothing)",
                output: TypeDesc {
                    name: "Nullable",
                    nullable: false,
                    args: vec![TypeDesc {
                        name: "Nothing",
                        nullable: false,
                        args: vec![],
                    }],
                },
            },
            TestCase {
                desc: "empty arg",
                input: "DateTime()",
                output: TypeDesc {
                    name: "DateTime",
                    nullable: false,
                    args: vec![],
                },
            },
            TestCase {
                desc: "numeric arg",
                input: "FixedString(42)",
                output: TypeDesc {
                    name: "FixedString",
                    nullable: false,
                    args: vec![TypeDesc {
                        name: "42",
                        nullable: false,
                        args: vec![],
                    }],
                },
            },
            TestCase {
                desc: "multiple args",
                input: "Array(Tuple(Tuple(String, String), Tuple(String, UInt64)))",
                output: TypeDesc {
                    name: "Array",
                    nullable: false,
                    args: vec![TypeDesc {
                        name: "Tuple",
                        nullable: false,
                        args: vec![
                            TypeDesc {
                                name: "Tuple",
                                nullable: false,
                                args: vec![
                                    TypeDesc {
                                        name: "String",
                                        nullable: false,
                                        args: vec![],
                                    },
                                    TypeDesc {
                                        name: "String",
                                        nullable: false,
                                        args: vec![],
                                    },
                                ],
                            },
                            TypeDesc {
                                name: "Tuple",
                                nullable: false,
                                args: vec![
                                    TypeDesc {
                                        name: "String",
                                        nullable: false,
                                        args: vec![],
                                    },
                                    TypeDesc {
                                        name: "UInt64",
                                        nullable: false,
                                        args: vec![],
                                    },
                                ],
                            },
                        ],
                    }],
                },
            },
            TestCase {
                desc: "map args",
                input: "Map(String, Array(Int64))",
                output: TypeDesc {
                    name: "Map",
                    nullable: false,
                    args: vec![
                        TypeDesc {
                            name: "String",
                            nullable: false,
                            args: vec![],
                        },
                        TypeDesc {
                            name: "Array",
                            nullable: false,
                            args: vec![TypeDesc {
                                name: "Int64",
                                nullable: false,
                                args: vec![],
                            }],
                        },
                    ],
                },
            },
            TestCase {
                desc: "map nullable value args",
                input: "Nullable(Map(String, String NULL))",
                output: TypeDesc {
                    name: "Nullable",
                    nullable: false,
                    args: vec![TypeDesc {
                        name: "Map",
                        nullable: false,
                        args: vec![
                            TypeDesc {
                                name: "String",
                                nullable: false,
                                args: vec![],
                            },
                            TypeDesc {
                                name: "String",
                                nullable: true,
                                args: vec![],
                            },
                        ],
                    }],
                },
            },
        ];
        for case in test_cases {
            let output = parse_type_desc(case.input).unwrap();
            assert_eq!(output, case.output, "{}", case.desc);
        }
    }

    #[test]
    fn test_parse_complex_type_with_null() {
        struct TestCase<'t> {
            desc: &'t str,
            input: &'t str,
            output: TypeDesc<'t>,
        }
        let test_cases = vec![
            TestCase {
                desc: "complex nullable type",
                input: "Nullable(Tuple(String NULL, Array(Tuple(Array(Int32 NULL) NULL, Array(String NULL) NULL) NULL) NULL))",
                output: TypeDesc {
                    name: "Nullable",
                    nullable: false,
                    args: vec![
                        TypeDesc {
                            name: "Tuple",
                            nullable: false,
                            args: vec![
                                TypeDesc {
                                    name: "String",
                                    nullable: true,
                                    args: vec![],
                                },
                                TypeDesc {
                                    name: "Array",
                                    nullable: true,
                                    args: vec![
                                        TypeDesc{
                                            name: "Tuple",
                                            nullable: true,
                                            args: vec![
                                                TypeDesc {
                                                    name: "Array",
                                                    nullable: true,
                                                    args: vec![
                                                        TypeDesc {
                                                            name: "Int32",
                                                            nullable: true,
                                                            args: vec![],
                                                        },
                                                    ],
                                                },
                                                TypeDesc {
                                                    name: "Array",
                                                    nullable: true,
                                                    args: vec![
                                                        TypeDesc {
                                                            name: "String",
                                                            nullable: true,
                                                            args: vec![],
                                                        },
                                                    ],
                                                },
                                            ]
                                        }
                                    ],
                                },
                            ],
                        },
                    ],
                },
            },
        ];
        for case in test_cases {
            let output = parse_type_desc(case.input).unwrap();
            assert_eq!(output, case.output, "{}", case.desc);
        }
    }
}
