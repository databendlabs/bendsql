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

use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::{Cell, CellAlignment, Color, Table};
use databend_driver::Schema;
use databend_driver::{Row, RowStatsIterator, RowWithStats, SchemaRef, ServerStats, Value};
use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};
use terminal_size::terminal_size;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::ast::QueryKind;
use crate::{
    ast::{format_query, highlight_query},
    config::{ExpandMode, OutputFormat, OutputQuoteStyle, Settings},
};

pub(crate) const INTERRUPTED_MESSAGE: &str = "Interrupted by Ctrl+C";
const HEAD_YELLOW: Color = Color::DarkBlue;

const DEFAULT_MAX_WIDTH: usize = 120;
const MIN_MAX_WIDTH: usize = 80;
const MIN_MAX_COL_WIDTH: usize = 10;

const DOT: &str = "·";
const DOTDOTDOT: &str = "…";

const NULL_WIDTH: usize = 4;
const TRUE_WIDTH: usize = 4;
const FALSE_WIDTH: usize = 5;
const EMPTY_WIDTH: usize = 2;
const DATE_WIDTH: usize = 10;
const TIMESTAMP_WIDTH: usize = 26;

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self, expand: Option<ExpandMode>) -> Result<ServerStats>;
}

pub struct FormatDisplay<'a> {
    settings: &'a Settings,
    query: &'a str,
    kind: QueryKind,
    quote_string: bool,
    data: RowStatsIterator,

    rows_count: usize,
    progress: Option<ProgressBar>,
    start: Instant,
    stats: Option<ServerStats>,
    interrupted: Arc<AtomicBool>,
}

impl<'a> FormatDisplay<'a> {
    pub fn new(
        settings: &'a Settings,
        query: &'a str,
        quote_string: bool,
        start: Instant,
        data: RowStatsIterator,
        interrupted: Arc<AtomicBool>,
    ) -> Self {
        Self {
            settings,
            query,
            kind: QueryKind::from(query),
            quote_string,
            data,
            rows_count: 0,
            progress: None,
            start,
            stats: None,
            interrupted,
        }
    }
}

impl FormatDisplay<'_> {
    fn running_secs(&self) -> f64 {
        // prefer to show server running time
        if let Some(ref stats) = self.stats {
            stats.running_time_ms / 1000.0
        } else {
            self.start.elapsed().as_secs_f64()
        }
    }

    async fn display_progress(&mut self, ss: &ServerStats) {
        if self.settings.show_progress {
            let pb = self.progress.take();
            match self.kind {
                QueryKind::Get(_, _) | QueryKind::Query => {
                    self.progress = Some(display_progress(pb, ss, "read"));
                }
                QueryKind::Put(_, _) | QueryKind::Update => {
                    self.progress = Some(display_progress(pb, ss, "write"));
                }
                _ => {}
            }
        }
    }

    async fn display_table(&mut self, expand: Option<ExpandMode>) -> Result<()> {
        if self.settings.display_pretty_sql {
            let format_sql = format_query(self.query);
            let format_sql = highlight_query(&format_sql);
            println!("\n{format_sql}\n");
        }

        let expand = expand.unwrap_or(self.settings.expand);
        // If in expand mode or query kind is Explain, Graphical, or ShowCreate,
        // collect all rows without early discarding.
        let collect_all_rows = matches!(expand, ExpandMode::On)
            || matches!(
                self.kind,
                QueryKind::Explain | QueryKind::Graphical | QueryKind::ShowCreate
            );
        let max_display_top_rows = self.settings.max_display_rows / 2
            + (!self.settings.max_display_rows.is_multiple_of(2)) as usize;
        let max_display_bottom_rows = self.settings.max_display_rows / 2;
        let mut rows = Vec::new();
        let mut bottom_rows = VecDeque::new();
        let mut error = None;
        while let Some(line) = self.data.next().await {
            if self.interrupted.load(Ordering::SeqCst) {
                return Err(anyhow!(INTERRUPTED_MESSAGE));
            }
            match line {
                Ok(RowWithStats::Row(row)) => {
                    if collect_all_rows || self.rows_count < max_display_top_rows {
                        rows.push(row);
                    } else {
                        bottom_rows.push_back(row);
                        // Since bendsql only displays the maximum number of rows,
                        // we can discard some rows to avoid data occupying too much memory.
                        if bottom_rows.len() > max_display_bottom_rows {
                            bottom_rows.pop_front();
                        }
                    }
                    self.rows_count += 1;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.display_progress(&ss).await;
                    self.stats = Some(ss);
                }
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }
        // collect bottom rows
        while let Some(row) = bottom_rows.pop_front() {
            rows.push(row);
        }

        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        if let Some(err) = error {
            return Err(anyhow!(
                "error happens after fetched {} rows: {}",
                rows.len(),
                err
            ));
        }
        if rows.is_empty() {
            return Ok(());
        }

        if self.kind == QueryKind::Explain {
            print_explain(&rows)?;
            return Ok(());
        }

        let schema = self.data.schema();
        if self.kind == QueryKind::ShowCreate {
            print_expanded(schema, &rows)?;
            return Ok(());
        }

        match expand {
            ExpandMode::On => {
                print_expanded(schema, &rows)?;
            }
            ExpandMode::Off => {
                println!(
                    "{}",
                    create_table(
                        schema,
                        &rows,
                        self.quote_string,
                        self.settings.max_width,
                        self.settings.max_col_width,
                        self.rows_count
                    )?
                );
            }
            ExpandMode::Auto => {
                // FIXME: depends on terminal size
                println!(
                    "{}",
                    create_table(
                        schema,
                        &rows,
                        self.quote_string,
                        self.settings.max_width,
                        self.settings.max_col_width,
                        self.rows_count
                    )?
                );
            }
        }

        Ok(())
    }

    async fn display_csv(&mut self) -> Result<()> {
        let quote_style = match self.settings.quote_style {
            OutputQuoteStyle::Always => csv::QuoteStyle::Always,
            OutputQuoteStyle::Necessary => csv::QuoteStyle::Necessary,
            OutputQuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
            OutputQuoteStyle::Never => csv::QuoteStyle::Never,
        };
        let mut wtr = csv::WriterBuilder::new()
            .quote_style(quote_style)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            if self.interrupted.load(Ordering::SeqCst) {
                return Err(anyhow!(INTERRUPTED_MESSAGE));
            }
            match line {
                Ok(RowWithStats::Row(row)) => {
                    self.rows_count += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.stats = Some(ss);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    async fn display_tsv(&mut self) -> Result<()> {
        let quote_style = match self.settings.quote_style {
            OutputQuoteStyle::Always => csv::QuoteStyle::Always,
            OutputQuoteStyle::Necessary => csv::QuoteStyle::Necessary,
            OutputQuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
            OutputQuoteStyle::Never => csv::QuoteStyle::Never,
        };
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .quote(b'"')
            .quote_style(quote_style)
            .from_writer(std::io::stdout());
        while let Some(line) = self.data.next().await {
            if self.interrupted.load(Ordering::SeqCst) {
                return Err(anyhow!(INTERRUPTED_MESSAGE));
            }
            match line {
                Ok(RowWithStats::Row(row)) => {
                    self.rows_count += 1;
                    let record = row.into_iter().map(|v| v.to_string()).collect::<Vec<_>>();
                    wtr.write_record(record)?;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.stats = Some(ss);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    async fn display_null(&mut self) -> Result<()> {
        let mut error = None;
        while let Some(line) = self.data.next().await {
            if self.interrupted.load(Ordering::SeqCst) {
                return Err(anyhow!(INTERRUPTED_MESSAGE));
            }
            match line {
                Ok(RowWithStats::Row(_)) => {
                    self.rows_count += 1;
                }
                Ok(RowWithStats::Stats(ss)) => {
                    self.display_progress(&ss).await;
                    self.stats = Some(ss);
                }
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }
        if let Some(pb) = self.progress.take() {
            pb.finish_and_clear();
        }
        if let Some(err) = error {
            return Err(anyhow!(
                "error happens after fetched {} rows: {}",
                self.rows_count,
                err
            ));
        }
        Ok(())
    }

    async fn display_stats(&mut self) {
        if !self.settings.show_stats {
            return;
        }

        if let Some(ref mut stats) = self.stats {
            stats.normalize();

            let (rows, mut rows_str, kind, total_rows, total_bytes) = match self.kind {
                QueryKind::Graphical => (self.rows_count, "rows", "graphical", 0, 0),
                QueryKind::Explain => (self.rows_count, "rows", "explain", 0, 0),
                QueryKind::ShowCreate => (self.rows_count, "rows", "showcreate", 0, 0),
                QueryKind::Query => (
                    self.rows_count,
                    "rows",
                    "read",
                    stats.read_rows,
                    stats.read_bytes,
                ),
                QueryKind::Update | QueryKind::AlterUserPassword | QueryKind::GenData(_, _, _) => (
                    stats.write_rows,
                    "rows",
                    "written",
                    stats.write_rows,
                    stats.write_bytes,
                ),
                QueryKind::Get(_, _) => (
                    stats.read_rows,
                    "files",
                    "downloaded",
                    stats.read_rows,
                    stats.read_bytes,
                ),
                QueryKind::Put(_, _) => (
                    stats.write_rows,
                    "files",
                    "uploaded",
                    stats.write_rows,
                    stats.write_bytes,
                ),
            };
            let mut rows_speed_str = rows_str;
            if rows <= 1 {
                rows_str = rows_str.trim_end_matches('s');
            }
            let rows_speed = total_rows as f64 / self.running_secs();
            if rows_speed <= 1.0 {
                rows_speed_str = rows_speed_str.trim_end_matches('s');
            }
            eprintln!(
                "{} {} {} in {:.3} sec. Processed {} {}, {} ({} {}/s, {}/s)",
                rows,
                rows_str,
                kind,
                self.running_secs(),
                humanize_count(total_rows as f64),
                rows_str,
                HumanBytes(total_bytes as u64),
                humanize_count(rows_speed),
                rows_speed_str,
                HumanBytes((total_bytes as f64 / self.running_secs()) as u64),
            );
            eprintln!();
        }
    }
}

#[async_trait::async_trait]
impl ChunkDisplay for FormatDisplay<'_> {
    async fn display(&mut self, expand: Option<ExpandMode>) -> Result<ServerStats> {
        if self.interrupted.load(Ordering::SeqCst) {
            return Err(anyhow!(INTERRUPTED_MESSAGE));
        }

        match self.settings.output_format {
            OutputFormat::Table => {
                self.display_table(expand).await?;
            }
            OutputFormat::CSV => {
                self.display_csv().await?;
            }
            OutputFormat::TSV => {
                self.display_tsv().await?;
            }
            OutputFormat::Null => {
                self.display_null().await?;
            }
        }
        self.display_stats().await;
        let stats = self.stats.take().unwrap_or_default();
        Ok(stats)
    }
}

fn format_read_progress(ss: &ServerStats, elapsed: f64) -> String {
    format!(
        "Processing {}/{} ({} rows/s), {}/{} ({}/s){}",
        humanize_count(ss.read_rows as f64),
        humanize_count(ss.total_rows as f64),
        humanize_count(ss.read_rows as f64 / elapsed),
        HumanBytes(ss.read_bytes as u64),
        HumanBytes(ss.total_bytes as u64),
        HumanBytes((ss.read_bytes as f64 / elapsed) as u64),
        if ss.spill_file_nums > 0 {
            format!(
                ", spilled {} files, {}",
                ss.spill_file_nums,
                HumanBytes(ss.spill_bytes as u64)
            )
        } else {
            "".to_string()
        }
    )
}

pub fn format_write_progress(ss: &ServerStats, elapsed: f64) -> String {
    format!(
        "Written {} ({} rows/s), {} ({}/s){}",
        humanize_count(ss.write_rows as f64),
        humanize_count(ss.write_rows as f64 / elapsed),
        HumanBytes(ss.write_bytes as u64),
        HumanBytes((ss.write_bytes as f64 / elapsed) as u64),
        if ss.spill_file_nums > 0 {
            format!(
                ", spilled {} files, {}",
                ss.spill_file_nums,
                HumanBytes(ss.spill_bytes as u64)
            )
        } else {
            "".to_string()
        }
    )
}

fn display_progress(pb: Option<ProgressBar>, current: &ServerStats, kind: &str) -> ProgressBar {
    let pb = pb.unwrap_or_else(|| {
        let pbn = ProgressBar::new(current.total_bytes as u64);
        let progress_color = "green";
        let template = "{spinner:.${progress_color}} [{elapsed_precise}] {msg} {wide_bar:.${progress_color}/blue} ({eta})".replace("${progress_color}", progress_color);
        pbn.set_style(
            ProgressStyle::with_template(&template)
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                })
                .progress_chars("█▓▒░ "),
        );
        pbn
    });

    pb.set_position(current.read_bytes as u64);
    match kind {
        "read" => pb.set_message(format_read_progress(current, pb.elapsed().as_secs_f64())),
        "write" => pb.set_message(format_write_progress(current, pb.elapsed().as_secs_f64())),
        _ => {}
    }
    pb
}

/// Convert a series of rows into a table
fn create_table(
    schema: SchemaRef,
    results: &[Row],
    quote_string: bool,
    mut max_width: usize,
    mut max_col_width: usize,
    rows_count: usize,
) -> Result<Table> {
    let mut table = Table::new();
    table
        .load_preset("││──├─┼┤│    ──┌┐└┘")
        .apply_modifier(UTF8_ROUND_CORNERS);

    // TODO customize the HorizontalLines in settings
    // table.set_style(TableComponent::HorizontalLines, '-');
    table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);

    let w = terminal_size();
    if max_width == u16::MAX as usize {
        table.set_content_arrangement(comfy_table::ContentArrangement::Disabled);
    } else {
        if max_width == 0 {
            if let Some((w, _)) = w {
                max_width = w.0 as usize;
            } else {
                max_width = DEFAULT_MAX_WIDTH;
            }
        }
        // max widths can not under 80
        max_width = max_width.max(MIN_MAX_WIDTH);
        table.set_width(max_width as _);
    }
    // max col widths can not under 10
    max_col_width = max_col_width.max(MIN_MAX_COL_WIDTH);

    if results.is_empty() {
        return Ok(table);
    }

    let value_rows_count: usize = results.len();
    let (top_rows, bottom_rows) = if value_rows_count == rows_count {
        (value_rows_count, 0usize)
    } else {
        let top_rows = value_rows_count / 2 + (!value_rows_count.is_multiple_of(2)) as usize;
        (top_rows, value_rows_count - top_rows)
    };

    let column_widths =
        compute_column_widths(&schema, results, max_width, max_col_width, quote_string);

    let mut aligns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields().iter() {
        if field.data_type.is_numeric() {
            aligns.push(CellAlignment::Right);
        } else {
            aligns.push(CellAlignment::Left);
        }
    }

    let mut res_vec: Vec<Vec<Cell>> = Vec::with_capacity(results.len());
    // Render top rows.
    for row in results.iter().take(top_rows) {
        let values = row.values();
        let mut cells = Vec::with_capacity(values.len());
        for (value, (column_width, align)) in
            values.iter().zip(column_widths.iter().zip(aligns.iter()))
        {
            let cell = format_table_style(value, *column_width, quote_string, *align);
            cells.push(cell);
        }
        res_vec.push(cells);
    }

    if bottom_rows != 0 {
        // Render blank rows to indicate the omitted intermediate rows.
        let mut cells = Vec::with_capacity(schema.fields().len());
        for align in aligns.iter() {
            let cell = Cell::new(DOT).set_alignment(*align);
            cells.push(cell);
        }
        for _ in 0..3 {
            res_vec.push(cells.clone());
        }

        // Render bottom rows.
        for row in results.iter().skip(top_rows) {
            let values = row.values();
            let mut cells = Vec::with_capacity(values.len());
            for (value, (column_width, align)) in
                values.iter().zip(column_widths.iter().zip(aligns.iter()))
            {
                let cell = format_table_style(value, *column_width, quote_string, *align);
                cells.push(cell);
            }
            res_vec.push(cells);
        }
    }

    let is_single_number_result = results.len() == 1
        && results[0].values().iter().all(|v| {
            if matches!(v, Value::Number(_)) {
                let f: f64 = v.to_string().parse().unwrap();
                f >= 1_000_000f64
            } else {
                false
            }
        });

    if is_single_number_result {
        let mut cells = Vec::new();
        for (idx, cell) in res_vec[0].iter().enumerate() {
            let f: f64 = cell.content().parse().unwrap();
            let content = format!("({})", humanize_count(f));
            let cell = Cell::new(&content)
                .fg(Color::Rgb {
                    r: 128,
                    g: 128,
                    b: 128,
                })
                .set_alignment(aligns[idx]);
            cells.push(cell);
        }
        res_vec.push(cells);
    }

    let column_count = schema.fields().len();
    let mut header = Vec::with_capacity(column_count);

    render_head(schema, &column_widths, &mut header);
    table.set_header(header);

    for cells in res_vec.into_iter() {
        table.add_row(cells);
    }

    if bottom_rows != 0 {
        let rows_count_str = format!("{rows_count} rows");
        let show_count_str = format!("({} shown)", top_rows + bottom_rows);
        table.add_row(vec![Cell::new(rows_count_str).set_alignment(aligns[0])]);
        table.add_row(vec![Cell::new(show_count_str).set_alignment(aligns[0])]);
    }

    Ok(table)
}

fn render_head(schema: SchemaRef, col_widths: &[usize], header: &mut Vec<Cell>) {
    let fields = schema.fields();
    for (field, col_width) in fields.iter().zip(col_widths.iter()) {
        let field_name = truncate_string(field.name.to_string(), *col_width);
        let field_data_type = truncate_string(field.data_type.to_string(), *col_width);

        let head_name = format!("{field_name}\n{field_data_type}");
        let cell = Cell::new(head_name)
            .fg(HEAD_YELLOW)
            .set_alignment(CellAlignment::Center);

        header.push(cell);
    }
}

fn print_expanded(schema: SchemaRef, results: &[Row]) -> Result<()> {
    let mut head_width = 0;
    for field in schema.fields() {
        if field.name.len() > head_width {
            head_width = field.name.len();
        }
    }
    for (row, result) in results.iter().enumerate() {
        println!(
            "*************************** {}. row ***************************",
            row + 1
        );
        for (idx, field) in schema.fields().iter().enumerate() {
            println!("{: >head_width$}: {}", field.name, result.values()[idx]);
        }
    }
    println!();
    Ok(())
}

fn print_explain(results: &[Row]) -> Result<()> {
    println!("-[ EXPLAIN ]-----------------------------------");
    for result in results {
        println!("{}", result.values()[0]);
    }
    println!();
    Ok(())
}

pub fn humanize_count(num: f64) -> String {
    if num == 0.0 {
        return String::from("0");
    }

    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = [
        "",
        " thousand",
        " million",
        " billion",
        " trillion",
        " quadrillion",
    ];

    if num < 1_f64 {
        return format!("{negative}{num:.2}");
    }
    let delimiter = 1000_f64;
    let exponent = std::cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{negative}{pretty_bytes}{unit}")
}

fn format_table_style(
    value: &Value,
    max_col_width: usize,
    quote_string: bool,
    align: CellAlignment,
) -> Cell {
    let is_null = matches!(value, Value::Null);
    let is_string = matches!(value, Value::String(_));
    let mut value_str = value.to_string();
    if is_string && quote_string {
        let mut escaped_value_str = String::with_capacity(value_str.len());
        for c in value_str.chars() {
            match c {
                '\\' => escaped_value_str.push_str("\\\\"),
                '\n' => escaped_value_str.push_str("\\n"),
                '\t' => escaped_value_str.push_str("\\t"),
                '\r' => escaped_value_str.push_str("\\r"),
                '\0' => escaped_value_str.push_str("\\0"),
                '\'' => escaped_value_str.push_str("\\'"),
                _ => escaped_value_str.push(c),
            }
        }
        value_str = escaped_value_str;
    }
    value_str = truncate_string(value_str, max_col_width);
    if is_string && quote_string {
        value_str = format!("'{value_str}'");
    }

    // Set the color of NULL values to dark gray to distinguish them from string NULL values.
    if is_null {
        Cell::new(value_str)
            .set_alignment(align)
            .fg(Color::DarkGrey)
    } else {
        Cell::new(value_str).set_alignment(align)
    }
}

fn compute_column_widths(
    schema: &Schema,
    results: &[Row],
    mut max_width: usize,
    max_col_width: usize,
    quote_string: bool,
) -> Vec<usize> {
    let column_num = schema.fields().len();
    // The maximum width must subtract the width of border and line within each column.
    max_width -= column_num * 3 + 1;

    let mut column_widths: Vec<usize> = Vec::with_capacity(column_num);
    // Collect the width of each column header
    for field in schema.fields() {
        let type_str = field.data_type.to_string();
        let width = field.name.len().max(type_str.len());
        column_widths.push(width);
    }

    // Collect the maximum width of each column value
    for row in results.iter() {
        let values = row.values();
        for (i, value) in values.iter().enumerate() {
            let width = value_display_width(value, quote_string);
            if width > column_widths[i] {
                column_widths[i] = width;
            }
        }
    }

    let mut total_width: usize = column_widths.iter().sum();
    // If the sum of all column widths exceeds the maximum width limit,
    // we need to reduce the width of some columns and truncate the corresponding data.
    if total_width > max_width {
        for value_width in column_widths.iter_mut() {
            if *value_width <= max_col_width {
                continue;
            } else if total_width <= max_width {
                break;
            }

            let total_width_diff = total_width - max_width;
            let value_width_diff = *value_width - max_col_width;
            if total_width_diff > value_width_diff {
                *value_width = max_col_width;
                total_width -= value_width_diff;
            } else {
                *value_width -= total_width_diff;
                break;
            }
        }
    }

    column_widths
}

fn truncate_string(value: String, col_width: usize) -> String {
    let value_width = UnicodeWidthStr::width(value.as_str());
    if value_width <= col_width {
        return value;
    }
    let element_size = col_width.saturating_sub(1);
    String::from_utf8(
        value
            .graphemes(true)
            .take(element_size)
            .flat_map(|g| g.as_bytes().iter())
            .copied() // copied converts &u8  4324324324324;
            .chain(DOTDOTDOT.as_bytes().iter().copied())
            .collect::<Vec<u8>>(),
    )
    .unwrap()
}

fn value_display_width(value: &Value, quote_string: bool) -> usize {
    match value {
        Value::Null => NULL_WIDTH,
        Value::Boolean(b) => {
            if *b {
                TRUE_WIDTH
            } else {
                FALSE_WIDTH
            }
        }
        Value::EmptyArray => EMPTY_WIDTH,
        Value::EmptyMap => EMPTY_WIDTH,
        Value::Date(_) => DATE_WIDTH,
        Value::Timestamp(_, _) => TIMESTAMP_WIDTH,
        Value::String(_) => {
            let value_str = value.to_string();
            if quote_string {
                let mut width = UnicodeWidthStr::width(value_str.as_str());
                // add quotes length
                width += 2;
                for c in value_str.chars() {
                    if matches!(c, '\\' | '\n' | '\t' | '\r' | '\0' | '\'') {
                        width += 1;
                    }
                }
                width
            } else {
                UnicodeWidthStr::width(value_str.as_str())
            }
        }
        _ => {
            let value_str = value.to_string();
            UnicodeWidthStr::width(value_str.as_str())
        }
    }
}
