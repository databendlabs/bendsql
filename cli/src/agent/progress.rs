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

use std::time::Duration;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// A terminal-only, indeterminate activity indicator. Drop it before writing the
/// answer/error, and on abandoned requests, so no spinner survives in the output.
pub(crate) struct AiProgress {
    bar: ProgressBar,
}

impl AiProgress {
    pub(crate) fn new() -> Self {
        Self::with_target(ProgressDrawTarget::stderr())
    }

    fn with_target(target: ProgressDrawTarget) -> Self {
        let bar = ProgressBar::with_draw_target(None, target);
        bar.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}").expect("static spinner template"),
        );
        bar.set_message("AI thinking");
        if !bar.is_hidden() {
            bar.tick();
            bar.enable_steady_tick(Duration::from_millis(100));
        }
        Self { bar }
    }
}

impl Drop for AiProgress {
    fn drop(&mut self) {
        self.bar.disable_steady_tick();
        self.bar.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use indicatif::TermLike;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct RecordingTerm(Arc<Mutex<Vec<String>>>);

    impl TermLike for RecordingTerm {
        fn width(&self) -> u16 {
            80
        }
        fn move_cursor_up(&self, _: usize) -> io::Result<()> {
            Ok(())
        }
        fn move_cursor_down(&self, _: usize) -> io::Result<()> {
            Ok(())
        }
        fn move_cursor_right(&self, _: usize) -> io::Result<()> {
            Ok(())
        }
        fn move_cursor_left(&self, _: usize) -> io::Result<()> {
            Ok(())
        }
        fn write_line(&self, text: &str) -> io::Result<()> {
            self.write_str(text)
        }
        fn write_str(&self, text: &str) -> io::Result<()> {
            self.0.lock().unwrap().push(format!("write:{text}"));
            Ok(())
        }
        fn clear_line(&self) -> io::Result<()> {
            self.0.lock().unwrap().push("clear".into());
            Ok(())
        }
        fn flush(&self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn drop_stops_and_clears_before_final_output() {
        let term = RecordingTerm::default();
        let spinner =
            AiProgress::with_target(ProgressDrawTarget::term_like(Box::new(term.clone())));
        let bar = spinner.bar.clone();
        assert!(term
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.contains("AI thinking")));
        assert!(!bar.is_finished());
        drop(spinner);
        assert!(bar.is_finished());
        term.write_line("AI> answer").unwrap();
        let events = term.0.lock().unwrap();
        let cleared = events.iter().rposition(|s| s == "clear").unwrap();
        assert!(cleared < events.len() - 1);
        assert!(!events[cleared + 1..]
            .iter()
            .any(|s| s.contains("AI thinking")));
    }

    #[test]
    fn hidden_target_has_no_terminal_side_effects() {
        let spinner = AiProgress::with_target(ProgressDrawTarget::hidden());
        let bar = spinner.bar.clone();
        assert!(bar.is_hidden());
        drop(spinner);
        assert!(bar.is_finished());
    }
}
