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
use std::env;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::{Cell, CellAlignment, Color, Table};
use databend_driver::{Row, RowStatsIterator, RowWithStats, SchemaRef, ServerStats, Value};
use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};
use terminal_size::terminal_size;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use unicode_segmentation::UnicodeSegmentation;

use crate::ast::QueryKind;
use crate::{
    ast::{format_query, highlight_query},
    config::{ExpandMode, OutputFormat, OutputQuoteStyle, Settings},
    web::set_data,
};

pub(crate) const INTERRUPTED_MESSAGE: &str = "Interrupted by Ctrl+C";
const HEAD_YELLOW: Color = Color::DarkBlue;

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
    server_addr: Option<String>,
}

impl<'a> FormatDisplay<'a> {
    pub fn new(
        settings: &'a Settings,
        query: &'a str,
        quote_string: bool,
        start: Instant,
        data: RowStatsIterator,
        interrupted: Arc<AtomicBool>,
        server_addr: Option<String>,
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
            server_addr,
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

    async fn display_graphical(&mut self, rows: &[Row]) -> Result<()> {
        let addr = self
            .server_addr
            .clone()
            .ok_or(anyhow!("Server not started"))?;

        let mut result = String::new();
        for row in rows {
            result.push_str(&row.values()[0].to_string());
        }

        let perf_id = set_data(result);

        let url = format!("http://{}?perf_id={}", addr, perf_id);

        // Open the browser in a separate task if not in ssh mode
        let in_sshmode = env::var("SSH_CLIENT").is_ok() || env::var("SSH_TTY").is_ok();
        if !in_sshmode && self.settings.auto_open_browser {
            if let Err(e) = webbrowser::open(&url) {
                eprintln!("Failed to open browser: {}", e);
            }
        }

        println!("View graphical online: \x1B[4m{}\x1B[0m", url);
        println!();
        Ok(())
    }

    async fn display_table(&mut self, expand: Option<ExpandMode>) -> Result<()> {
        if self.settings.display_pretty_sql {
            let format_sql = format_query(self.query);
            let format_sql = highlight_query(&format_sql);
            println!("\n{}\n", format_sql);
        }

        let max_display_rows_count = self.settings.max_display_rows / 2 + 2;
        let mut rows = Vec::new();
        let mut bottom_rows = VecDeque::new();
        let mut error = None;
        while let Some(line) = self.data.next().await {
            if self.interrupted.load(Ordering::SeqCst) {
                return Err(anyhow!(INTERRUPTED_MESSAGE));
            }
            match line {
                Ok(RowWithStats::Row(row)) => {
                    if self.rows_count < max_display_rows_count {
                        rows.push(row);
                    } else {
                        bottom_rows.push_back(row);
                        // Since bendsql only displays the maximum number of rows,
                        // we can discard some rows to avoid data occupying too much memory.
                        if bottom_rows.len() > max_display_rows_count {
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

        if self.kind == QueryKind::Graphical {
            return self.display_graphical(&rows).await;
        }

        let schema = self.data.schema();
        if self.kind == QueryKind::ShowCreate {
            print_expanded(schema, &rows)?;
            return Ok(());
        }

        let expand = expand.unwrap_or(self.settings.expand);
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
                        self.settings.max_display_rows,
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
                        self.settings.max_display_rows,
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
    max_rows: usize,
    max_width: usize,
    max_col_width: usize,
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
    if max_width != 0 && max_width <= u16::MAX as usize {
        if let Some((w, _)) = w {
            let max_width = max_width.min(w.0 as usize);
            table.set_width(max_width as _);
        }
    }

    if results.is_empty() {
        return Ok(table);
    }

    let value_rows_count: usize = results.len();
    let mut rows_to_render = value_rows_count.min(max_rows);
    if !quote_string {
        rows_to_render = value_rows_count;
    } else if value_rows_count <= max_rows + 3 {
        // hiding rows adds 3 extra rows
        // so hiding rows makes no sense if we are only slightly over the limit
        // if we are 1 row over the limit hiding rows will actually increase the number of lines we display!
        // in this case render all the rows
        rows_to_render = value_rows_count;
    }

    let (top_rows, bottom_rows) = if rows_to_render == value_rows_count {
        (value_rows_count, 0usize)
    } else {
        let top_rows = rows_to_render / 2 + (rows_to_render % 2 != 0) as usize;
        (top_rows, rows_to_render - top_rows)
    };

    let mut res_vec: Vec<Vec<String>> = vec![];
    for row in results.iter().take(top_rows) {
        let values = row.values();
        let mut v = vec![];
        for value in values {
            v.push(format_table_style(value, max_col_width, quote_string));
        }
        res_vec.push(v);
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

    if bottom_rows != 0 {
        for row in results.iter().skip(value_rows_count - bottom_rows) {
            let values = row.values();
            let mut v = vec![];
            for value in values {
                v.push(format_table_style(value, max_col_width, quote_string));
            }
            res_vec.push(v);
        }
    }

    let column_count = schema.fields().len();
    let mut header = Vec::with_capacity(column_count);
    let mut aligns = Vec::with_capacity(column_count);

    render_head(schema, &mut header, &mut aligns);
    table.set_header(header);

    // render the top rows
    for values in res_vec.iter().take(top_rows) {
        let mut cells = Vec::new();
        for (idx, val) in values.iter().enumerate() {
            let cell = Cell::new(val).set_alignment(aligns[idx]);
            cells.push(cell);
        }
        table.add_row(cells);
    }

    // render the bottom rows
    if bottom_rows != 0 {
        // first render the divider
        let mut cells: Vec<Cell> = Vec::new();
        let display_res_len = res_vec.len();
        for align in aligns.iter() {
            let cell = Cell::new("·").set_alignment(*align);
            cells.push(cell);
        }

        for _ in 0..3 {
            table.add_row(cells.clone());
        }

        for values in res_vec.iter().skip(display_res_len - bottom_rows) {
            let mut cells = Vec::new();
            for (idx, val) in values.iter().enumerate() {
                let cell = Cell::new(val).set_alignment(aligns[idx]);
                cells.push(cell);
            }
            table.add_row(cells);
        }

        let rows_count_str = format!("{} rows", rows_count);
        let show_count_str = format!("({} shown)", top_rows + bottom_rows);
        table.add_row(vec![Cell::new(rows_count_str).set_alignment(aligns[0])]);
        table.add_row(vec![Cell::new(show_count_str).set_alignment(aligns[0])]);
    }

    if is_single_number_result {
        let mut cells = Vec::new();
        for (idx, value) in res_vec[0].iter().enumerate() {
            let f: f64 = value.parse().unwrap();
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
        table.add_row(cells);
    }

    Ok(table)
}

fn render_head(schema: SchemaRef, header: &mut Vec<Cell>, aligns: &mut Vec<CellAlignment>) {
    let fields = schema.fields();
    for field in fields.iter() {
        let field_name = field.name.to_string();
        let field_data_type = field.data_type.to_string();
        let head_name = format!("{}\n{}", field_name, field_data_type);
        let cell = Cell::new(head_name)
            .fg(HEAD_YELLOW)
            .set_alignment(CellAlignment::Center);

        header.push(cell);

        if field.data_type.is_numeric() {
            aligns.push(CellAlignment::Right);
        } else {
            aligns.push(CellAlignment::Left);
        }
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
        return format!("{}{:.2}", negative, num);
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
    format!("{}{}{}", negative, pretty_bytes, unit)
}

fn format_table_style(value: &Value, max_col_width: usize, quote_string: bool) -> String {
    let is_string = matches!(value, Value::String(_));
    let mut value = value.to_string();
    if is_string && quote_string {
        value = value
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
            .replace("\r", "\\r")
            .replace("\0", "\\0")
            .replace("'", "\\'");
    }
    if value.len() + 5 > max_col_width {
        let element_size = max_col_width.saturating_sub(6);
        value = String::from_utf8(
            value
                .graphemes(true)
                .take(element_size)
                .flat_map(|g| g.as_bytes().iter())
                .copied() // copied converts &u8  4324324324324;
                .chain(b"...".iter().copied())
                .collect::<Vec<u8>>(),
        )
        .unwrap();
    }
    if is_string && quote_string {
        value = format!("'{}'", value);
    }
    value
}
