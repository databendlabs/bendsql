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

//! Redacts credentials passed on the command line from the process-visible
//! argv, so other local users cannot read them through `ps` or
//! `/proc/<pid>/cmdline`.
//!
//! Every byte of a secret is overwritten in place with `*`. Lengths and NUL
//! separators never change, so the argv pointer table stays valid and `ps`
//! (which on macOS splits argv by NUL) still shows every argument. The secret
//! length stays visible. Credentials remain visible between `exec` and this call,
//! so `BENDSQL_PASSWORD` / `BENDSQL_DSN` are still the safer choice.

// Lists of byte ranges to mask often hold a single range by design.
#![allow(clippy::single_range_in_vec_init)]

use std::ffi::OsString;
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::ptr;

use clap::Command;
use percent_encoding::percent_decode;

/// Connection parameters (DSN query or `--set key=value`) that carry secrets.
const SENSITIVE_KEYS: &[&str] = &["access_token", "session_token"];

#[derive(Clone, Copy)]
enum Kind {
    /// The whole value is a secret.
    Secret,
    /// A DSN: mask the password and sensitive query parameters.
    Dsn,
    /// A `key=value` pair: mask the value if the key is sensitive.
    KeyValue,
}

struct OptSpec {
    short: Option<char>,
    long: Option<String>,
    takes_value: bool,
    require_equals: bool,
    kind: Option<Kind>,
}

fn sensitive_kind(id: &str) -> Option<Kind> {
    match id {
        "password" => Some(Kind::Secret),
        "dsn" => Some(Kind::Dsn),
        "set" => Some(Kind::KeyValue),
        _ => None,
    }
}

fn option_specs(mut cmd: Command) -> Vec<OptSpec> {
    cmd.build();
    cmd.get_arguments()
        .map(|arg| OptSpec {
            short: arg.get_short(),
            long: arg.get_long().map(str::to_string),
            takes_value: arg.get_action().takes_values(),
            require_equals: arg.is_require_equals_set(),
            kind: sensitive_kind(arg.get_id().as_str()),
        })
        .collect()
}

/// Rewrite argument `index` to `value`, which has the original length.
#[derive(Debug, PartialEq)]
struct Edit {
    index: usize,
    value: Vec<u8>,
}

/// Walks `args` the same way clap does and plans redactions for sensitive
/// option values. Only meaningful after clap has accepted `args`.
fn plan(specs: &[OptSpec], args: &[&[u8]]) -> Vec<Edit> {
    let mut edits = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let arg = args[i];
        i += 1;
        if arg == b"--" {
            break;
        }
        if let Some(body) = arg.strip_prefix(b"--") {
            let (name, attached) = match body.iter().position(|&b| b == b'=') {
                Some(eq) => (&body[..eq], true),
                None => (body, false),
            };
            let Some(spec) = specs
                .iter()
                .find(|s| s.long.as_deref().map(str::as_bytes) == Some(name))
            else {
                continue;
            };
            if attached {
                push_mask(&mut edits, args, i - 1, 2 + name.len() + 1, spec.kind);
            } else if spec.takes_value && !spec.require_equals && i < args.len() {
                push_mask(&mut edits, args, i, 0, spec.kind);
                i += 1;
            }
        } else if arg.len() > 1 && arg[0] == b'-' {
            // A cluster of short flags, e.g. `-nAp secret` or `-psecret`.
            let mut pos = 1;
            while pos < arg.len() {
                let c = char::from(arg[pos]);
                pos += 1;
                let Some(spec) = specs.iter().find(|s| s.short == Some(c)) else {
                    break;
                };
                if !spec.takes_value {
                    continue;
                }
                if pos < arg.len() {
                    // clap strips one `=` from an attached short value.
                    let offset = if arg[pos] == b'=' { pos + 1 } else { pos };
                    push_mask(&mut edits, args, i - 1, offset, spec.kind);
                } else if !spec.require_equals && i < args.len() {
                    push_mask(&mut edits, args, i, 0, spec.kind);
                    i += 1;
                }
                break;
            }
        }
    }
    edits
}

/// Masks the value starting at `args[index][offset..]` according to `kind`.
fn push_mask(
    edits: &mut Vec<Edit>,
    args: &[&[u8]],
    index: usize,
    offset: usize,
    kind: Option<Kind>,
) {
    let Some(kind) = kind else {
        return;
    };
    let arg = args[index];
    let value = &arg[offset..];
    let ranges = match kind {
        Kind::Secret => vec![0..value.len()],
        Kind::Dsn => dsn_secret_ranges(value),
        Kind::KeyValue => key_value_secret_ranges(value),
    };
    let masked = mask_ranges(value, &ranges);
    if masked != value {
        let mut new = arg[..offset].to_vec();
        new.extend(masked);
        edits.push(Edit { index, value: new });
    }
}

/// Overwrites every byte in `ranges` with `*`, preserving the length.
fn mask_ranges(value: &[u8], ranges: &[Range<usize>]) -> Vec<u8> {
    let mut out = value.to_vec();
    for r in ranges {
        out[r.clone()].fill(b'*');
    }
    out
}

fn is_sensitive_key(key: &[u8]) -> bool {
    let key = percent_decode(key).decode_utf8_lossy();
    SENSITIVE_KEYS.contains(&key.as_ref())
}

/// Byte ranges of the password and sensitive query values in a DSN like
/// `databend://user:pass@host:8000/db?access_token=xxx`.
fn dsn_secret_ranges(dsn: &[u8]) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let auth_start = find(dsn, b"://").map_or(0, |p| p + 3);
    let head_end = dsn[auth_start..]
        .iter()
        .position(|&b| matches!(b, b'?' | b'#'))
        .map_or(dsn.len(), |p| auth_start + p);
    // The userinfo ends at the last `@` before the query; the password follows
    // the first `:`. Searching past `/` over-masks rather than leaking a
    // password that contains an unescaped `/`.
    let head = &dsn[auth_start..head_end];
    if let Some(at) = head.iter().rposition(|&b| b == b'@') {
        if let Some(colon) = head[..at].iter().position(|&b| b == b':') {
            ranges.push(auth_start + colon + 1..auth_start + at);
        }
    }
    if dsn.get(head_end) == Some(&b'?') {
        let query_start = head_end + 1;
        let query_end = dsn[query_start..]
            .iter()
            .position(|&b| b == b'#')
            .map_or(dsn.len(), |p| query_start + p);
        let mut start = query_start;
        for pair in dsn[query_start..query_end].split(|&b| b == b'&') {
            ranges.extend(
                key_value_secret_ranges(pair)
                    .into_iter()
                    .map(|r| start + r.start..start + r.end),
            );
            start += pair.len() + 1;
        }
    }
    ranges
}

/// Range of the value in `key=value` when `key` is sensitive.
fn key_value_secret_ranges(pair: &[u8]) -> Vec<Range<usize>> {
    match pair.iter().position(|&b| b == b'=') {
        Some(eq) if is_sensitive_key(&pair[..eq]) => vec![eq + 1..pair.len()],
        _ => vec![],
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Locates the live argv strings: `(start, capacity)` per argument, where
/// `capacity` excludes the trailing NUL. Returns `None` unless the memory is
/// verified to hold exactly `args`.
#[cfg(target_os = "linux")]
fn argv_buffers(args: &[&[u8]]) -> Option<Vec<(*mut u8, usize)>> {
    use std::fs;

    // Fields 48/49 of /proc/self/stat are arg_start/arg_end (Linux >= 3.5).
    // Skip past `comm`, which is parenthesized and may contain spaces.
    let stat = fs::read("/proc/self/stat").ok()?;
    let rest = &stat[stat.iter().rposition(|&b| b == b')')? + 1..];
    let fields: Vec<&[u8]> = rest
        .split(|b| b.is_ascii_whitespace())
        .filter(|f| !f.is_empty())
        .collect();
    // `rest` starts at field 3 (state).
    let parse =
        |n: usize| -> Option<usize> { std::str::from_utf8(fields.get(n - 3)?).ok()?.parse().ok() };
    let (arg_start, arg_end) = (parse(48)?, parse(49)?);

    let expected: Vec<u8> = args
        .iter()
        .flat_map(|a| a.iter().chain(&[0]))
        .copied()
        .collect();
    if arg_end.checked_sub(arg_start)? != expected.len()
        || fs::read("/proc/self/cmdline").ok()? != expected
    {
        return None;
    }

    let mut offset = arg_start;
    Some(
        args.iter()
            .map(|a| {
                let buf = (offset as *mut u8, a.len());
                offset += a.len() + 1;
                buf
            })
            .collect(),
    )
}

#[cfg(target_os = "macos")]
fn argv_buffers(args: &[&[u8]]) -> Option<Vec<(*mut u8, usize)>> {
    use std::ffi::{c_char, c_int, CStr};

    extern "C" {
        fn _NSGetArgc() -> *mut c_int;
        fn _NSGetArgv() -> *mut *mut *mut c_char;
    }

    // SAFETY: both functions return pointers to process-global argc/argv set
    // up by dyld, valid for the lifetime of the process.
    unsafe {
        let argc = usize::try_from(*_NSGetArgc()).ok()?;
        let argv = *_NSGetArgv();
        if argc != args.len() || argv.is_null() {
            return None;
        }
        (0..argc)
            .map(|i| {
                let p = *argv.add(i);
                if p.is_null() || CStr::from_ptr(p).to_bytes() != args[i] {
                    return None;
                }
                Some((p.cast::<u8>(), args[i].len()))
            })
            .collect()
    }
}

/// Redacts credentials from the process-visible argv. Best effort: does
/// nothing if the argv memory cannot be located and verified.
///
/// Call right after clap has parsed the arguments; anything that reads
/// `std::env::args()` afterwards sees the masked values.
pub fn mask_credentials(cmd: Command) {
    let owned: Vec<OsString> = std::env::args_os().collect();
    let args: Vec<&[u8]> = owned.iter().map(|a| a.as_bytes()).collect();
    let edits = plan(&option_specs(cmd), &args);
    if edits.is_empty() {
        return;
    }
    let Some(buffers) = argv_buffers(&args) else {
        return;
    };
    for edit in edits {
        let (dst, len) = buffers[edit.index];
        if edit.value.len() != len {
            continue;
        }
        // SAFETY: `dst..dst + len` is the verified, writable argv string for
        // this argument; its NUL terminator at `dst + len` is left intact.
        // No Rust references point into it; `owned` holds copies.
        unsafe {
            ptr::copy_nonoverlapping(edit.value.as_ptr(), dst, len);
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::*;
    use crate::Args;

    /// Applies `plan` to `argv` and returns the resulting arguments. Also
    /// checks that clap accepts the input and that lengths are preserved.
    fn masked(argv: &[&str]) -> Vec<String> {
        Args::try_parse_from(argv).expect("clap should accept test argv");
        let args: Vec<&[u8]> = argv.iter().map(|a| a.as_bytes()).collect();
        let mut out: Vec<String> = argv.iter().map(|a| a.to_string()).collect();
        for edit in plan(&option_specs(Args::command()), &args) {
            assert_eq!(edit.value.len(), args[edit.index].len());
            out[edit.index] = String::from_utf8(edit.value).unwrap();
        }
        out
    }

    #[test]
    fn masks_password_forms() {
        assert_eq!(
            masked(&["bendsql", "-p", "secret", "-u", "root"]),
            ["bendsql", "-p", "******", "-u", "root"]
        );
        assert_eq!(masked(&["bendsql", "-psecret"]), ["bendsql", "-p******"]);
        assert_eq!(masked(&["bendsql", "-p=secret"]), ["bendsql", "-p=******"]);
        assert_eq!(
            masked(&["bendsql", "--password", "secret"]),
            ["bendsql", "--password", "******"]
        );
        assert_eq!(
            masked(&["bendsql", "--password=secret"]),
            ["bendsql", "--password=******"]
        );
        // Short flag clusters: boolean flags before `p`.
        assert_eq!(
            masked(&["bendsql", "-nAp", "secret"]),
            ["bendsql", "-nAp", "******"]
        );
        assert_eq!(
            masked(&["bendsql", "-nApsecret"]),
            ["bendsql", "-nAp******"]
        );
    }

    #[test]
    fn leaves_non_secrets_alone() {
        let argv = ["bendsql", "-u", "secret", "--query=-p x", "-n", "--time"];
        assert_eq!(masked(&argv), argv);
        // `-h` takes a value, so `p...` here is the host, not a password.
        assert_eq!(masked(&["bendsql", "-hpost"]), ["bendsql", "-hpost"]);
        // A value that looks like a flag name is consumed as a value.
        assert_eq!(
            masked(&["bendsql", "-D", "p", "-u=-p", "-p", "x"]),
            ["bendsql", "-D", "p", "-u=-p", "-p", "*"]
        );
        assert_eq!(masked(&["bendsql", "-p", ""]), ["bendsql", "-p", ""]);
    }

    #[test]
    fn masks_dsn() {
        assert_eq!(
            masked(&[
                "bendsql",
                "--dsn",
                "databend://root:secret@host:8000/db?sslmode=disable"
            ]),
            [
                "bendsql",
                "--dsn",
                "databend://root:******@host:8000/db?sslmode=disable"
            ]
        );
        assert_eq!(
            masked(&[
                "bendsql",
                "--dsn=databend://u:p%40ss@h/?access_token=tok&role=r#x"
            ]),
            [
                "bendsql",
                "--dsn=databend://u:******@h/?access_token=***&role=r#x"
            ]
        );
        assert_eq!(
            masked(&["bendsql", "--dsn", "databend://u:pa/ss@h"]),
            ["bendsql", "--dsn", "databend://u:*****@h"]
        );
        assert_eq!(
            masked(&["bendsql", "--dsn", "databend+flight://u@h?access%5Ftoken=t"]),
            ["bendsql", "--dsn", "databend+flight://u@h?access%5Ftoken=*"]
        );
        let argv = ["bendsql", "--dsn", "databend://u@h:8000/db?role=r"];
        assert_eq!(masked(&argv), argv);
    }

    #[test]
    fn masks_sensitive_set_values() {
        assert_eq!(
            masked(&[
                "bendsql",
                "--set",
                "access_token=t",
                "--set=session_token=s",
                "--set",
                "role=r"
            ]),
            [
                "bendsql",
                "--set",
                "access_token=*",
                "--set=session_token=*",
                "--set",
                "role=r"
            ]
        );
    }

    #[test]
    fn mask_preserves_length() {
        assert_eq!(mask_ranges(b"abc", &[1..1]), b"abc");
        assert_eq!(mask_ranges(b"a:bb@c", &[2..4]), b"a:**@c");
        // Multi-byte secrets become one `*` per byte.
        assert_eq!(mask_ranges("p:密@h".as_bytes(), &[2..5]), b"p:***@h");
    }
}
