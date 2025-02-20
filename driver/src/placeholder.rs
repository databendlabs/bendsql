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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnPosition;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::Statement;
use databend_common_ast::Range;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use crate::Params;

#[derive(Visitor)]
#[visitor(Expr(enter), Identifier(enter), ColumnRef(enter))]
pub(crate) struct PlaceholderVisitor {
    placeholders: Vec<Range>,
    column_positions: Vec<(usize, Range)>,
    names: Vec<(String, Range)>,
}

impl PlaceholderVisitor {
    pub fn new() -> Self {
        PlaceholderVisitor {
            placeholders: vec![],
            column_positions: vec![],
            names: Vec::new(),
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Hole {
                name,
                span: Some(range),
            } => {
                self.names.push((name.clone(), *range));
            }
            Expr::Placeholder { span: Some(range) } => {
                self.placeholders.push(*range);
            }
            _ => {}
        }
    }

    fn enter_identifier(&mut self, ident: &Identifier) {
        if let (IdentifierType::Hole, Some(range)) = (ident.ident_type, ident.span) {
            self.names.push((ident.name.clone(), range));
        }
    }

    fn enter_column_ref(&mut self, r: &ColumnRef) {
        if let ColumnID::Position(ColumnPosition {
            span: Some(range),
            pos,
            ..
        }) = r.column
        {
            self.column_positions.push((pos, range));
        }
    }

    pub fn replace_sql(&mut self, params: &Params, stmt: &Statement, sql: &str) -> String {
        stmt.drive(self);
        self.placeholders.sort_by(|l, r| l.start.cmp(&r.start));

        let mut results = vec![];

        for (index, range) in self.placeholders.iter().enumerate() {
            if let Some(v) = params.get_by_index(index + 1) {
                results.push((v.to_string(), *range));
            }
        }

        for (name, range) in self.names.iter() {
            if let Some(v) = params.get_by_name(name) {
                results.push((v.to_string(), *range));
            }
        }

        let mut sql = sql.to_string();
        if !results.is_empty() {
            results.sort_by(|a, b| a.1.start.cmp(&b.1.start));
            for (value, r) in results.iter().rev() {
                let start = r.start as usize;
                let end = r.end as usize;
                sql.replace_range(start..end, value);
            }
        }

        if !self.column_positions.is_empty() {
            self.column_positions
                .sort_by(|a, b| a.1.start.cmp(&b.1.start));

            for (index, r) in self.column_positions.iter().rev() {
                if let Some(value) = params.get_by_index(*index) {
                    let start = r.start as usize;
                    let end = r.end as usize;
                    sql.replace_range(start..end, value);
                }
            }
        }

        sql
    }
}
