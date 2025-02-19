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

use std::vec;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::Statement;
use databend_common_ast::Range;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use crate::Params;

#[derive(Visitor)]
#[visitor(Expr(enter), Identifier(enter))]
pub(crate) struct PlaceholderVisitor {
    place_holders: Vec<Range>,
    names: Vec<(String, Range)>,
}

impl PlaceholderVisitor {
    pub fn new() -> Self {
        PlaceholderVisitor {
            place_holders: vec![],
            names: Vec::new(),
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Hole {
                name,
                span: Some(range),
            } => {
                self.names.push((name.clone(), range.clone()));
            }
            Expr::Placeholder { span: Some(range) } => {
                self.place_holders.push(range.clone());
            }
            _ => {}
        }
    }

    fn enter_identifier(&mut self, ident: &Identifier) {
        match (ident.ident_type, ident.span) {
            (IdentifierType::Hole, Some(range)) => {
                self.names.push((ident.name.clone(), range));
            }
            _ => {}
        }
    }

    pub fn replace_sql(&mut self, params: &Params, stmt: &Statement, sql: &str) -> String {
        stmt.drive(self);
        self.place_holders.sort_by(|l, r| l.start.cmp(&r.start));

        let mut results = vec![];

        for (index, range) in self.place_holders.iter().enumerate() {
            if let Some(v) = params.get_by_index(index + 1) {
                results.push((v.to_string(), range.clone()));
            }
        }

        for (name, range) in self.names.iter() {
            if let Some(v) = params.get_by_name(name) {
                results.push((v.to_string(), range.clone()));
            }
        }

        if !results.is_empty() {
            let mut sql = sql.to_string();
            results.sort_by(|a, b| a.1.start.cmp(&b.1.start));
            for (value, r) in results.iter().rev() {
                let start = r.start as usize;
                let end = r.end as usize;
                sql.replace_range(start..end, value);
            }
            return sql;
        }
        sql.to_string()
    }
}
