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

use databend_common_ast::parser::token::{TokenKind, Tokenizer};

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace0;
use nom::multi::separated_list0;
use nom::number::float;
use nom::Parser;
use nom::{bytes::complete::take_while, character::complete::char, IResult};

// alter current user's password tokens
const ALTER_USER_PASSWORD_TOKENS: [TokenKind; 6] = [
    TokenKind::USER,
    TokenKind::USER,
    TokenKind::LParen,
    TokenKind::RParen,
    TokenKind::IDENTIFIED,
    TokenKind::BY,
];

#[derive(PartialEq, Debug)]
pub enum QueryKind {
    Query,
    Update,
    Explain,
    Put(String, String),
    Get(String, String),
    // gendata(tpch, scale = 1, override = 1)
    GenData(GenType, f32, bool),
    AlterUserPassword,
    Graphical,
    ShowCreate,
}

#[derive(PartialEq, Eq, Debug)]
pub enum GenType {
    TPCH,
    TPCDS,
}

impl From<&str> for QueryKind {
    fn from(query: &str) -> Self {
        let mut tz = Tokenizer::new(query);
        match tz.next() {
            Some(Ok(t)) => match t.kind {
                TokenKind::EXPLAIN => {
                    if query.to_lowercase().contains("graphical") {
                        QueryKind::Graphical
                    } else {
                        QueryKind::Explain
                    }
                }
                TokenKind::SHOW => match tz.next() {
                    Some(Ok(t)) if t.kind == TokenKind::CREATE => QueryKind::ShowCreate,
                    _ => QueryKind::Query,
                },
                TokenKind::PUT => {
                    let args: Vec<String> = query
                        .split_ascii_whitespace()
                        .skip(1)
                        .map(|x| x.to_owned())
                        .collect();
                    if args.len() == 2 {
                        QueryKind::Put(args[0].clone(), args[1].clone())
                    } else {
                        QueryKind::Query
                    }
                }
                TokenKind::GET => {
                    let args: Vec<String> = query
                        .split_ascii_whitespace()
                        .skip(1)
                        .map(|x| x.to_owned())
                        .collect();
                    if args.len() == 2 {
                        QueryKind::Get(args[0].clone(), args[1].clone())
                    } else {
                        QueryKind::Query
                    }
                }
                TokenKind::ALTER => {
                    let mut tzs = vec![];
                    while let Some(Ok(t)) = tz.next() {
                        tzs.push(t.kind);
                        if tzs.len() == ALTER_USER_PASSWORD_TOKENS.len() {
                            break;
                        }
                    }
                    if tzs == ALTER_USER_PASSWORD_TOKENS {
                        QueryKind::AlterUserPassword
                    } else {
                        QueryKind::Update
                    }
                }
                TokenKind::DELETE
                | TokenKind::UPDATE
                | TokenKind::INSERT
                | TokenKind::CREATE
                | TokenKind::DROP
                | TokenKind::OPTIMIZE => QueryKind::Update,

                _ => gendata_parser(query)
                    .map(|(_, k)| k)
                    .unwrap_or(QueryKind::Query),
            },
            _ => QueryKind::Query,
        }
    }
}

pub fn replace_newline_in_box_display(query: &str) -> bool {
    let mut tz = Tokenizer::new(query);
    match tz.next() {
        Some(Ok(t)) => match t.kind {
            TokenKind::EXPLAIN => false,
            TokenKind::SHOW => !matches!(tz.next(), Some(Ok(t)) if t.kind == TokenKind::CREATE),
            _ => true,
        },
        _ => true,
    }
}

// Define the parser for the GenType
fn gen_type(input: &str) -> IResult<&str, GenType> {
    let (input, gen_type_str) = alt((tag_no_case("tpch"), tag_no_case("tpcds"))).parse(input)?;
    let gen_type = match gen_type_str.to_ascii_lowercase().as_str() {
        "tpch" => GenType::TPCH,
        "tpcds" => GenType::TPCDS,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )))
        }
    };
    Ok((input, gen_type))
}

// Define the parser for the key-value pair (e.g., "scale = 100")
fn key_value(input: &str) -> IResult<&str, (&str, f32)> {
    let (input, _) = multispace0(input)?;
    let (input, key) = take_while(|c: char| c.is_alphabetic())(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = float().parse(input)?;
    Ok((input, (key, value)))
}

// Define the parser for the entire gendata function
fn gendata_parser(input: &str) -> IResult<&str, QueryKind> {
    let (input, _) = tag_no_case("gendata")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse the GenType
    let (input, gen_type) = gen_type(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse the key-value pairs
    let (input, key_values) = separated_list0(char(','), key_value).parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Extract scale and override from key_values
    let mut scale = 0f32;
    let mut override_val = false;
    for (key, value) in key_values {
        match key.to_ascii_lowercase().as_str() {
            "sf" | "scale" => scale = value,
            "override" => override_val = value > 0.0,
            _ => {}
        }
    }

    Ok((
        input,
        QueryKind::GenData(gen_type, scale as _, override_val),
    ))
}

#[cfg(test)]
mod test {
    use super::QueryKind;

    #[test]
    fn test_query_kind() {
        let cases = vec![
            (
                QueryKind::from("gendata(tpch, scale = 1, override = 0)"),
                QueryKind::GenData(super::GenType::TPCH, 1.0f32, false),
            ),
            (
                QueryKind::from("gendata(tpcds, scale = 10, override = 1)"),
                QueryKind::GenData(super::GenType::TPCDS, 10.0f32, true),
            ),
            (
                QueryKind::from("gendata(tpcds, scale = 0.1, override = 1)"),
                QueryKind::GenData(super::GenType::TPCDS, 0.1f32, true),
            ),
        ];

        for (l, r) in cases {
            assert_eq!(l, r)
        }
    }
}
