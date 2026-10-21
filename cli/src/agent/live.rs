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

//! Live presentation of a streamed AI answer.
//!
//! The partial answer is drawn into a transient region that is never taller
//! than the terminal, so it never scrolls into scrollback and can always be
//! erased with relative cursor movement. Only the tail is shown while the
//! answer grows. The region is erased before the final answer or error is
//! printed, so a failed or cancelled turn leaves nothing behind.

use std::io::{self, Write};

use unicode_width::UnicodeWidthChar;

use super::markdown;
use super::memory::MAX_TEXT_BYTES;
use super::progress::AiProgress;
use crate::config::AiOutput;

const MIN_COLUMNS: usize = 20;
const MIN_ROWS: usize = 5;
// Synchronized output: supporting terminals apply a frame atomically, others
// ignore the private mode.
const FRAME_START: &str = "\x1b[?2026h";
const FRAME_END: &str = "\x1b[?2026l";
const RESET: &str = "\x1b[0m";

/// Spinner plus optional live region for one AI request.
pub(crate) struct AnswerView {
    live: Option<LiveRegion<io::Stdout>>,
    spinner: Option<AiProgress>,
    _echo: Option<EchoOff>,
}

impl AnswerView {
    /// Live output needs a capable Unix terminal of a usable size; otherwise
    /// the spinner stays until the final answer, as without streaming.
    pub(crate) fn new(mode: AiOutput, stream: bool) -> Self {
        let live = if stream && cfg!(unix) && markdown::rich_terminal() {
            stdout_size()
                .filter(|&(columns, rows)| columns >= MIN_COLUMNS && rows >= MIN_ROWS)
                .map(|size| LiveRegion::new(io::stdout(), mode, markdown::color(), size))
        } else {
            None
        };
        Self {
            _echo: live.as_ref().and_then(|_| EchoOff::new()),
            live,
            spinner: Some(AiProgress::new()),
        }
    }

    pub(crate) fn is_live(&self) -> bool {
        self.live.is_some()
    }

    pub(crate) fn push(&mut self, delta: &str) {
        if let Some(live) = &mut self.live {
            live.push(delta);
        }
    }

    /// Draw pending text. Call periodically, not per delta, to bound redraws.
    pub(crate) fn tick(&mut self) {
        let Some(live) = &mut self.live else {
            return;
        };
        if stdout_size() != Some(live.size) {
            // Some terminals reflow drawn rows on resize, so the row count is
            // no longer reliable. Erase what we can and finish with the spinner.
            let _ = live.clear();
            self.live = None;
            self.spinner.get_or_insert_with(AiProgress::new);
            return;
        }
        if live.has_frame() {
            // Join the spinner ticker and erase its line before the first frame.
            self.spinner = None;
            if live.draw().is_err() {
                let _ = live.clear();
                self.live = None;
            }
        }
    }

    /// Erase every transient line; call before printing the answer or error.
    pub(crate) fn finish(self) {}
}

impl Drop for AnswerView {
    fn drop(&mut self) {
        self.spinner = None;
        if let Some(live) = &mut self.live {
            let _ = live.clear();
        }
    }
}

/// Between readline calls the terminal is in cooked mode, so type-ahead (and
/// the `^C` echo) would be written into the live region and desync its row
/// count. Disable local echo while streaming; input stays buffered for the
/// next prompt and Ctrl+C still raises SIGINT.
struct EchoOff {
    #[cfg(unix)]
    original: libc::termios,
}

impl EchoOff {
    #[cfg(unix)]
    fn new() -> Option<Self> {
        let fd = libc::STDIN_FILENO;
        // SAFETY: termios is plain data and the calls only access stdin.
        unsafe {
            if libc::isatty(fd) != 1 {
                return None;
            }
            let mut original = std::mem::zeroed::<libc::termios>();
            if libc::tcgetattr(fd, &mut original) != 0 {
                return None;
            }
            let mut quiet = original;
            quiet.c_lflag &= !(libc::ECHO | libc::ECHONL);
            (libc::tcsetattr(fd, libc::TCSANOW, &quiet) == 0).then_some(Self { original })
        }
    }

    #[cfg(not(unix))]
    fn new() -> Option<Self> {
        None
    }
}

impl Drop for EchoOff {
    fn drop(&mut self) {
        // SAFETY: restores the attributes read in `new` on the same descriptor.
        #[cfg(unix)]
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.original);
        }
    }
}

fn stdout_size() -> Option<(usize, usize)> {
    terminal_size::terminal_size_of(io::stdout())
        .map(|(width, height)| (usize::from(width.0), usize::from(height.0)))
}

struct LiveRegion<W: Write> {
    out: W,
    mode: AiOutput,
    color: bool,
    /// Terminal (columns, rows) when streaming started.
    size: (usize, usize),
    text: String,
    dirty: bool,
    /// Physical rows currently drawn; the cursor rests at the end of the last.
    rows: usize,
}

impl<W: Write> LiveRegion<W> {
    fn new(out: W, mode: AiOutput, color: bool, size: (usize, usize)) -> Self {
        Self {
            out,
            mode,
            color,
            size,
            text: String::new(),
            dirty: false,
            rows: 0,
        }
    }

    /// Accumulate display text up to the 8 KiB answer budget.
    fn push(&mut self, delta: &str) {
        let room = MAX_TEXT_BYTES.saturating_sub(self.text.len());
        let mut end = room.min(delta.len());
        while !delta.is_char_boundary(end) {
            end -= 1;
        }
        if end > 0 {
            self.text.push_str(&delta[..end]);
            self.dirty = true;
        }
    }

    fn has_frame(&self) -> bool {
        self.dirty && !self.text.trim().is_empty()
    }

    fn draw(&mut self) -> io::Result<()> {
        let (columns, rows) = self.size;
        // Stay off the last column so the terminal never wraps a row itself,
        // and leave one row spare so the region never reaches scrollback.
        let rendered = markdown::render_frame(&self.text, self.mode, self.color);
        let frame = frame_rows(&rendered, columns - 1, rows - 1, self.color);
        let mut output = String::from(FRAME_START);
        output.push_str(&self.rewind());
        output.push_str(&frame.join("\r\n"));
        output.push_str(FRAME_END);
        self.out.write_all(output.as_bytes())?;
        self.out.flush()?;
        self.rows = frame.len();
        self.dirty = false;
        Ok(())
    }

    fn clear(&mut self) -> io::Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let output = self.rewind();
        self.rows = 0;
        self.out.write_all(output.as_bytes())?;
        self.out.flush()
    }

    /// Move to the first column of the region and erase to the end of screen.
    fn rewind(&self) -> String {
        match self.rows {
            0 | 1 => "\r\x1b[J".into(),
            rows => format!("\r\x1b[{}A\x1b[J", rows - 1),
        }
    }
}

/// The tail of `rendered` in at most `max_rows` rows of at most `width`
/// columns, with a marker row when earlier lines are hidden.
fn frame_rows(rendered: &str, width: usize, max_rows: usize, color: bool) -> Vec<String> {
    let mut rows = wrap(rendered, width);
    if rows.len() <= max_rows {
        return rows;
    }
    let keep = max_rows - 1;
    let hidden = rows.len() - keep;
    let tail = rows.split_off(hidden);
    let note = clip(
        &format!("… {hidden} earlier lines; the full answer is shown when complete"),
        width,
    );
    let note = if color {
        format!("\x1b[2m{note}{RESET}")
    } else {
        note
    };
    std::iter::once(note).chain(tail).collect()
}

fn clip(text: &str, width: usize) -> String {
    let mut used = 0;
    text.chars()
        .take_while(|c| {
            used += c.width().unwrap_or(0);
            used <= width
        })
        .collect()
}

/// Wrap rendered lines to `width` display columns. Tabs are expanded and SGR
/// styles are re-applied on continuation rows, so each row is self-contained.
fn wrap(rendered: &str, width: usize) -> Vec<String> {
    let mut rows = Vec::new();
    let mut active = String::new();
    for line in rendered.strip_suffix('\n').unwrap_or(rendered).split('\n') {
        let mut row = active.clone();
        let mut column = 0;
        let mut chars = line.char_indices().peekable();
        while let Some((start, c)) = chars.next() {
            if c == '\x1b' {
                // Only the renderer's own SGR sequences can reach this point.
                let mut end = start + 1;
                for (index, next) in chars.by_ref() {
                    end = index + next.len_utf8();
                    if next != '[' && ('\x40'..='\x7e').contains(&next) {
                        break;
                    }
                }
                let sequence = &line[start..end];
                if sequence == RESET {
                    active.clear();
                } else {
                    active.push_str(sequence);
                }
                row.push_str(sequence);
                continue;
            }
            let (text, cells) = if c == '\t' {
                (" ", 8 - column % 8)
            } else {
                (&line[start..start + c.len_utf8()], 1)
            };
            // VS16 (U+FE0F) asks for emoji presentation, which many terminals
            // draw two columns wide even when the base is narrow (`⚠️`, `✔️`).
            // Over-counting only wraps early; under-counting would let the
            // terminal wrap a row itself and desync the region.
            let emoji = chars.peek().is_some_and(|&(_, next)| next == '\u{FE0F}');
            for _ in 0..cells {
                let mut char_width = text.chars().next().and_then(|c| c.width()).unwrap_or(0);
                if emoji && char_width == 1 {
                    char_width = 2;
                }
                if char_width > 0 && column + char_width > width {
                    end_row(&mut rows, row, &active);
                    row = active.clone();
                    column = 0;
                }
                row.push_str(text);
                column += char_width;
            }
        }
        end_row(&mut rows, row, &active);
    }
    rows
}

fn end_row(rows: &mut Vec<String>, mut row: String, active: &str) {
    if !active.is_empty() {
        row.push_str(RESET);
    }
    rows.push(row);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn display_width(row: &str) -> usize {
        let mut width = 0;
        let mut escape = false;
        for c in row.chars() {
            if escape {
                escape = !('\x40'..='\x7e').contains(&c) || c == '[';
            } else if c == '\x1b' {
                escape = true;
            } else {
                width += c.width().unwrap_or(0);
            }
        }
        width
    }

    /// Minimal terminal: fixed size, scrolls at the bottom, and panics if the
    /// cursor would reach the last column, where autowrap would desync rows.
    struct Screen {
        size: (usize, usize),
        lines: Vec<Vec<char>>,
        row: usize,
        column: usize,
    }

    impl Screen {
        fn new(size: (usize, usize), lines: &[&str]) -> Self {
            let mut screen = Self {
                size,
                lines: vec![Vec::new(); size.1],
                row: 0,
                column: 0,
            };
            for (index, line) in lines.iter().enumerate() {
                screen.lines[index] = line.chars().collect();
            }
            screen.row = lines.len() - 1;
            screen.column = screen.lines[screen.row].len();
            screen
        }

        fn apply(&mut self, output: &[u8]) {
            let text = std::str::from_utf8(output).unwrap();
            let mut chars = text.chars().peekable();
            while let Some(c) = chars.next() {
                match c {
                    '\x1b' => {
                        assert_eq!(chars.next(), Some('['));
                        let mut parameters = String::new();
                        let action = loop {
                            let next = chars.next().unwrap();
                            if ('\x40'..='\x7e').contains(&next) {
                                break next;
                            }
                            parameters.push(next);
                        };
                        let count = parameters.parse::<usize>().unwrap_or(1);
                        match action {
                            'A' => self.row = self.row.saturating_sub(count),
                            'J' => {
                                self.lines[self.row].truncate(self.column);
                                for line in &mut self.lines[self.row + 1..] {
                                    line.clear();
                                }
                            }
                            'm' | 'h' | 'l' => {}
                            other => panic!("unexpected CSI {other}"),
                        }
                    }
                    '\r' => self.column = 0,
                    '\n' => {
                        if self.row + 1 == self.size.1 {
                            self.lines.remove(0);
                            self.lines.push(Vec::new());
                        } else {
                            self.row += 1;
                        }
                    }
                    c => {
                        let width = c.width().unwrap_or(0);
                        assert!(self.column + width < self.size.0, "row reached last column");
                        // Frames always write after erasing, left to right.
                        self.lines[self.row].push(c);
                        self.column += width;
                    }
                }
            }
        }

        fn text(&self) -> String {
            let lines: Vec<String> = self.lines.iter().map(|l| l.iter().collect()).collect();
            lines.join("\n").trim_end().to_owned()
        }
    }

    #[test]
    fn wrap_counts_display_columns_and_restores_styles() {
        let rows = wrap("中文中文中文\tx\n\x1b[1mbold text here\x1b[0m end\n", 7);
        for row in &rows {
            assert!(display_width(row) <= 7, "{row:?}");
        }
        assert_eq!(rows[0], "中文中");
        assert_eq!(rows[1], "文中文 ");
        // A tab that crosses the edge continues on the next row.
        assert_eq!(rows[2], " x");
        assert_eq!(rows[3], "\x1b[1mbold te\x1b[0m");
        // The style continues on the wrapped row and is reset after it.
        assert_eq!(rows[4], "\x1b[1mxt here\x1b[0m");
        assert_eq!(rows[5], " end");
        assert_eq!(wrap("a\n\nb", 10), ["a", "", "b"]);
        // Emoji presentation sequences count as two columns and stay together.
        assert_eq!(wrap("ab⚠\u{FE0F}c", 3), ["ab", "⚠\u{FE0F}c"]);
    }

    #[test]
    fn long_frames_show_only_the_tail_within_the_terminal() {
        let rendered: String = (0..30).map(|n| format!("line {n}\n")).collect();
        let rows = frame_rows(&rendered, 19, 9, false);
        assert_eq!(rows.len(), 9);
        assert!(rows[0].starts_with("… 22 earlier lines"));
        assert!(display_width(&rows[0]) <= 19);
        assert_eq!(rows[8], "line 29");
        assert!(frame_rows("short\n", 19, 9, true).len() == 1);
    }

    #[test]
    fn redraws_and_clear_leave_no_residue_even_after_scrolling() {
        let size = (24, 6);
        // Start on the bottom row, so the first frames scroll the screen.
        let mut screen = Screen::new(size, &["one", "two", "three", "four", "five", "agent> q"]);
        screen.apply(b"\r\n");
        let mut region = LiveRegion::new(Vec::new(), AiOutput::Plain, false, size);
        for delta in [
            "中文 answer ",
            "grows across ",
            "several wrapped rows ",
            "and more\nnew line\nx\ny\nz",
        ] {
            region.push(delta);
            region.draw().unwrap();
            assert!(region.rows < size.1);
            screen.apply(&std::mem::take(&mut region.out));
            let rendered = markdown::render_frame(&region.text, AiOutput::Plain, false);
            let last = wrap(&rendered, size.0 - 1).pop().unwrap();
            assert!(
                screen.text().ends_with(last.trim_end()),
                "{}",
                screen.text()
            );
        }
        let before_clear = screen.text();
        assert!(before_clear.contains("earlier lines"), "{before_clear}");
        region.clear().unwrap();
        screen.apply(&std::mem::take(&mut region.out));
        assert!(screen.text().ends_with("agent> q"), "{}", screen.text());
        assert!(!screen.text().contains("AI>"));
        // Clearing twice, or with nothing drawn, writes nothing.
        region.clear().unwrap();
        assert!(region.out.is_empty());
    }

    #[test]
    fn display_text_is_bounded_and_waits_for_printable_content() {
        let mut region = LiveRegion::new(Vec::new(), AiOutput::Markdown, false, (80, 24));
        region.push("  \n");
        assert!(!region.has_frame());
        // The budget never splits a multibyte character.
        region.push(&"中".repeat(MAX_TEXT_BYTES));
        assert!(region.has_frame());
        assert_eq!(region.text.len(), MAX_TEXT_BYTES - 2);
        region.push("ab");
        assert_eq!(region.text.len(), MAX_TEXT_BYTES);
        region.draw().unwrap();
        assert!(!region.has_frame());
        region.push("more");
        assert!(
            !region.has_frame(),
            "text beyond the budget is not displayed"
        );
    }

    #[test]
    fn frames_never_contain_model_control_sequences() {
        let mut region = LiveRegion::new(Vec::new(), AiOutput::Markdown, false, (80, 24));
        region.push("**x** \x1b]52;c;clip\x07\x1b[2J\r done");
        region.draw().unwrap();
        let output = String::from_utf8(region.out).unwrap();
        for forbidden in ["\x1b]", "\x07", "\x1b[2J"] {
            assert!(!output.contains(forbidden), "{output:?}");
        }
    }
}
