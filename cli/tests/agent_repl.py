# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unix PTY smoke test; no live Databend or model service required.

Run: python3 cli/tests/agent_repl.py target/debug/bendsql
"""

import argparse
import errno
import fcntl
import http.server
import json
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time

PROVIDER_ERRORS = {
    "provider-unauthorized", "provider-rate-limit", "provider-unavailable", "provider-invalid-json",
    "provider-empty-choices", "provider-no-text", "provider-oversized",
}


class MockService(http.server.BaseHTTPRequestHandler):
    sql_requests = []
    model_requests = []
    model_headers = []
    kill_requests = []
    page_requests = []
    release_queries = threading.Event()
    release_models = threading.Event()
    release_stream = threading.Event()
    delayed_query_ids = set()

    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length) or b"{}")
        status = 200
        if self.path.startswith("/v1/query/") and self.path.endswith("/kill"):
            self.kill_requests.append(self.path)
            query_id = self.path.split("/")[-2]
            if query_id in self.delayed_query_ids:
                self.release_queries.wait(10)
            response = {}
        elif self.path == "/v1/chat/completions":
            self.model_requests.append(request)
            self.model_headers.append({key.lower(): value for key, value in self.headers.items()})
            question = json.loads(request["messages"][-1]["content"])["question"]
            if question == "slow-spinner":
                time.sleep(0.5)
            if question == "error-spinner":
                time.sleep(0.3)
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if question == "wait-spinner":
                self.release_models.wait(10)
            # Standard non-streaming OpenAI Chat Completions shape; the CLI uses
            # the first assistant choice, not metadata, usage or service errors.
            answer = "Mock answer grounded in cached records."
            if question == "provider-markdown":
                answer = "## Query analysis\n\n**Estimated** rows are not actual timings.\n\n1. Inspect `EXPLAIN`.\n2. Check pruning.\n\n> Evidence only\n\n```sql\nSELECT 'a|b',\n  42;\n```\n\n[Docs](https://docs.databend.com/)\n\n<script>private-html-payload</script>\n\n![tracking](https://private.example/image)\n"
            elif question == "provider-suggest-sql":
                answer = "DROP TABLE orders;"
            elif question == "provider-controls":
                answer = "\x1b[31mPrintable answer\r\n"
            elif question == "provider-long-answer":
                answer = "中" * 8000
            if request.get("stream") and question not in PROVIDER_ERRORS:
                self.stream_answer(question, answer)
                return
            response = {
                "id": "chatcmpl-mock", "object": "chat.completion", "created": 1,
                "model": request.get("model"),
                "choices": [{"index": 0, "message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            }
            if question in ["provider-unauthorized", "provider-rate-limit", "provider-unavailable"]:
                status = {"provider-unauthorized": 401, "provider-rate-limit": 429, "provider-unavailable": 503}[question]
                response = {"error": {"message": "private-provider-diagnostic", "type": "mock_error", "code": str(status)}}
            elif question == "provider-invalid-json":
                response = b"private-provider-diagnostic is not JSON"
            elif question == "provider-empty-choices":
                response["choices"] = []
            elif question == "provider-no-text":
                response["choices"][0]["message"]["content"] = None
            elif question == "provider-oversized":
                response["choices"][0]["message"]["content"] = "private-provider-diagnostic" + "x" * (128 * 1024)
        elif self.path == "/v1/query":
            sql = request["sql"]
            self.sql_requests.append(sql)
            fail = sql.startswith("Not SQL") or "SPOOF_INTERRUPTED" in sql or "SENSITIVE_ERROR" in sql
            query_id = f"query-{len(self.sql_requests)}"
            if "BLOCK_SUBMISSION" in sql:
                self.release_queries.wait(10)
            if "STALL_KILL" in sql:
                self.delayed_query_ids.add(query_id)
            completion_query = "system.functions" in sql and "'f' as type" in sql
            data = (
                [["mock-version"]] if "version()" in sql.lower()
                else [["autocomplete_table_unique", "t"], ["autocomplete_column_unique", "c"]] if completion_query
                else [[str(n)] for n in range(50)]
            )
            response = {
                "id": query_id,
                "node_id": None,
                "session_id": None,
                "session": None,
                "schema": ([{"name": "name", "type": "String"}, {"name": "type", "type": "String"}]
                           if completion_query else [{"name": "password" if "SENSITIVE_PREVIEW" in sql else "value", "type": "String"}]),
                "data": [] if fail or "BLOCK_PAGE" in sql else [["private-result-secret"]] if "SENSITIVE_PREVIEW" in sql else data,
                "state": "Failed" if fail else "Running" if "BLOCK_PAGE" in sql else "Succeeded",
                "settings": None,
                "error": (
                    {"code": 1005, "message": "secret_access_key=private-error-secret" if "SENSITIVE_ERROR" in sql else "Interrupted by Ctrl+C in SPOOF_INTERRUPTED result" if "SPOOF_INTERRUPTED" in sql else "Mock syntax error", "detail": None}
                    if fail else None
                ),
                "warnings": None,
                "stats": {
                    "scan_progress": {"rows": 1000, "bytes": 8000},
                    "write_progress": {"rows": 0, "bytes": 0},
                    "result_progress": {"rows": len(data), "bytes": 100},
                    "spill_progress": {"file_nums": 0, "bytes": 0},
                    "running_time_ms": 10,
                    "total_scan": None,
                },
                "result_timeout_secs": None,
                "stats_uri": None,
                "final_uri": None,
                "next_uri": f"/v1/query/{query_id}/page/0" if "BLOCK_PAGE" in sql else None,
                "kill_uri": None,
            }
        else:
            self.send_error(404)
            return
        body = response if isinstance(response, bytes) else json.dumps(response, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client cancellation deliberately abandons pending responses.

    def stream_answer(self, question, answer):
        # OpenAI Chat Completions SSE: role chunk, content deltas, finish, [DONE].
        # Gated questions pause on release_stream after their first delta.
        gated = question in ["stream-gated", "stream-broken", "stream-wait", "stream-resize"]
        if question == "stream-gated":
            chunks = ["Streaming first part.", " Streaming second part."]
        elif question == "stream-broken":
            chunks = ["Partial streamed text"]
        elif question == "stream-wait":
            chunks = ["Waiting stream text", " never sent"]
        elif question == "stream-resize":
            chunks = ["Resize stream text", " completed after resize."]
        elif question == "stream-long":
            chunks = ["".join(f"row {n}\n" for n in range(start, start + 5)) for start in range(0, 40, 5)]
        else:
            third = max(1, len(answer) // 3)
            chunks = [answer[:third], answer[third:2 * third], answer[2 * third:]]

        def event(payload):
            self.wfile.write(b"data: " + json.dumps(payload, ensure_ascii=False).encode() + b"\n\n")
            self.wfile.flush()

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            event({"choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]})
            for index, chunk in enumerate(chunks):
                if gated and index == 1:
                    self.release_stream.wait(10)
                event({"choices": [{"index": 0, "delta": {"content": chunk}, "finish_reason": None}]})
                time.sleep(0.15 if question == "stream-long" else 0.05)
            if gated and len(chunks) == 1:
                self.release_stream.wait(10)
            if question == "stream-broken":
                return  # Connection closes without finish_reason or [DONE].
            event({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client cancellation deliberately abandons the stream.

    def do_GET(self):
        if not self.path.startswith("/v1/query/"):
            self.send_error(404)
            return
        self.page_requests.append(self.path)
        self.release_queries.wait(10)
        body = json.dumps({
            "id": self.path.split("/")[3], "state": "Succeeded", "schema": [], "data": [],
            "stats": {"scan_progress": {"rows": 1000, "bytes": 8000},
                      "write_progress": {"rows": 0, "bytes": 0},
                      "result_progress": {"rows": 0, "bytes": 0},
                      "spill_progress": {"file_nums": 0, "bytes": 0}, "running_time_ms": 10},
            "next_uri": None, "final_uri": None, "kill_uri": None,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


class Terminal:
    def __init__(self, executable, env, port, mode="smart", backend=None, autocomplete=False, startup_prompt=None, size=None):
        args = [
            executable, "--dsn",
            f"databend://root:@127.0.0.1:{port}/default?sslmode=disable&login=disable",
            "--output", "null",
        ]
        if not autocomplete:
            args.append("--no-auto-complete")
        if mode:
            args.extend(["--mode", mode])
        if backend:
            args.extend(["--backend", backend])
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execve(executable, args, env)
        if size:
            self.resize(*size)
        self.pending = b""
        self.transcript = []
        self.finished = False
        self.startup_prompt = startup_prompt or f"{mode or 'sql'}> "
        self.startup_checked = False
        self.bracketed_paste = env.get("TERM", "dumb") != "dumb"

    def expect(self, text):
        deadline = time.monotonic() + 15
        needle = text.encode()
        while needle not in self.pending:
            if time.monotonic() >= deadline:
                raise AssertionError(f"Timed out waiting for {text!r}")
            if select.select([self.fd], [], [], 0.1)[0]:
                try:
                    chunk = os.read(self.fd, 65536)
                except OSError as error:
                    if error.errno == errno.EIO:
                        raise AssertionError("CLI exited unexpectedly") from error
                    raise
                if not chunk:
                    raise AssertionError("CLI closed the terminal")
                self.pending += chunk
                self.transcript.append(chunk)
                if b"\x1b[6n" in chunk:
                    os.write(self.fd, b"\x1b[1;1R")
        end = self.pending.index(needle) + len(needle)
        self.pending = self.pending[end:]
        if text == self.startup_prompt and not self.startup_checked:
            output = b"".join(self.transcript)
            for verbose in [b"Interactive modes (", b"!mode [", b"Available backends:", b"Current backend:"]:
                assert verbose not in output, "Startup should not print usage or backend lists"
            self.startup_checked = True

    def pump(self, seconds):
        # Collect output without waiting for specific text.
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if select.select([self.fd], [], [], 0.05)[0]:
                chunk = os.read(self.fd, 65536)
                self.pending += chunk
                self.transcript.append(chunk)

    def resize(self, rows, columns):
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))

    def send(self, line, prompt="smart> "):
        os.write(self.fd, line.encode() + b"\r")
        if self.bracketed_paste:
            # SQL highlighting redraws the same prompt while typing. Wait for
            # readline exit/re-entry, not an earlier redraw, before sending more.
            self.expect("\x1b[?2004l")
            self.expect("\x1b[?2004h")
        self.expect(prompt)

    def finish(self):
        output = b"".join(self.transcript)
        for debug in [b"AI backend:", b"Codex: starting app-server.", b"Codex: generating response."]:
            assert debug not in output, "AI request debug messages must not pollute output"
        os.write(self.fd, b"quit\r")
        self.expect("Bye~")
        _, status = os.waitpid(self.pid, 0)
        self.finished = True
        assert os.waitstatus_to_exitcode(status) == 0

    def __enter__(self):
        return self

    def __exit__(self, error_type, *_):
        if error_type:
            print(b"".join(self.transcript).decode(errors="replace"), file=sys.stderr)
        if not self.finished:
            pid, _ = os.waitpid(self.pid, os.WNOHANG)
            if pid == 0:
                os.kill(self.pid, signal.SIGKILL)
                os.waitpid(self.pid, 0)
        os.close(self.fd)


def last_context():
    return json.loads(MockService.model_requests[-1]["messages"][-1]["content"])


def exercise_modes(executable, env, port):
    write_config(env, '[agent]\nbackend="env"')
    with Terminal(executable, env, port) as terminal:
        terminal.expect("smart> ")
        assert len(MockService.sql_requests) == 1  # Startup version query.
        terminal.send("SELECT 1;")
        assert len(MockService.sql_requests) == 2
        assert not MockService.model_requests
        terminal.send("!context")
        assert b"cached 30/50 rows" in b"".join(terminal.transcript)

        terminal.send("!mode agent", "agent> ")
        terminal.send("SELECT 2;", "agent> ")
        assert len(MockService.sql_requests) == 2
        assert len(MockService.model_requests) == 1
        record = last_context()["query_records"][0]
        assert record["id"] == "Q1" and record["status"] == "success"
        assert len(record["preview"]["rows"]) == 30
        assert record["preview"]["complete"] is False
        assert record["stats"]["read_rows"] == 1000

        terminal.send("!mode sql", "sql> ")
        terminal.send("Not SQL;", "sql> ")
        assert len(MockService.sql_requests) == 3
        assert len(MockService.model_requests) == 1
        terminal.send("!mode smart")
        terminal.send("!sql Not SQL;")
        terminal.send("!ask Explain Q2")
        failed = next(r for r in last_context()["query_records"] if r["id"] == "Q2")
        assert failed["status"] == "failed"
        assert "Mock syntax error" in failed["error"]

        terminal.send("SELECT * FROM", "smart(sql)> ")
        terminal.send("!mode agent", "smart(sql)> ")
        assert len(MockService.sql_requests) == 4
        assert len(MockService.model_requests) == 2
        terminal.send("numbers;")
        assert len(MockService.sql_requests) == 5
        terminal.send("!context clear")
        terminal.send("!context")
        terminal.send("!ask Hello after clearing")
        assert last_context()["query_records"] == []
        assert len(MockService.model_requests[-1]["messages"]) == 2
        terminal.send("SELECT 3;")
        terminal.send("!context")
        assert b"Q4: success" in b"".join(terminal.transcript)
        terminal.finish()


def exercise_missing_model(executable, env, port):
    env = {key: value for key, value in env.items() if not key.startswith("BENDSQL_AGENT_")}
    sql_count = len(MockService.sql_requests)
    model_count = len(MockService.model_requests)
    with Terminal(executable, env, port, mode="sql") as terminal:
        terminal.expect("sql> ")
        terminal.send("!ask Hello", "sql> ")
        assert b"Set BENDSQL_AGENT_BASE_URL" in b"".join(terminal.transcript)
        terminal.send("SELECT 1;", "sql> ")
        assert len(MockService.sql_requests) == sql_count + 2
        assert len(MockService.model_requests) == model_count
        terminal.finish()


def write_config(env, text, consent=True, standard_prompt=True):
    import re
    # Existing behavior tests explicitly authorize their mock backend. Privacy
    # tests opt out; no fake CLI/service should ever need interactive consent.
    if consent and "allow_data_sharing" not in text:
        if re.search(r"(?m)^\[agent\]\s*$", text):
            text = re.sub(r"(?m)^\[agent\]\s*$", "[agent]\\nallow_data_sharing=true", text, count=1)
        else:
            text = '[agent]\nallow_data_sharing=true\n' + text
    if standard_prompt:
        additions = []
        if not re.search(r"(?m)^prompt\s*=", text): additions.append('prompt="{mode}> "')
        if "continuation_prompt" not in text: additions.append('continuation_prompt="{mode}(sql)> "')
        if re.search(r"(?m)^\[settings\]\s*$", text):
            text = re.sub(r"(?m)^\[settings\]\s*$", "[settings]\\n" + "\\n".join(additions), text, count=1)
        elif additions:
            text = '[settings]\n' + '\n'.join(additions) + '\n' + text
    directory = os.path.join(env["HOME"], ".config", "bendsql")
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "config.toml"), "w") as file:
        file.write(text)


def exercise_configured_backends(executable, env, port):
    env = dict(env, SMOKE_API_KEY="private-token", SMOKE_HEADER_KEY="private-header")
    config = f"""
[agent]
backend = "first"
timeout_secs = 5
[agent.backends.first]
type = "openai-compatible"
base_url = "http://127.0.0.1:{port}/v1"
model = "first-model"
api_key_env = "SMOKE_API_KEY"
header_envs = {{ "X-Custom-Key" = "SMOKE_HEADER_KEY" }}
[agent.backends.second]
type = "openai-compatible"
base_url = "http://127.0.0.1:{port}/v1"
model = "second-model"
max_tokens = 1024
headers = {{ "x-project" = "example" }}
[agent.backends.broken]
type = "openai-compatible"
base_url = "https://user:private-url-secret@example.com/v1"
model = "broken-model"
"""
    write_config(env, config)
    with Terminal(executable, env, port, backend="second") as terminal:
        terminal.expect("smart> ")
        terminal.send("!backend")
        assert b"Current backend: second" in b"".join(terminal.transcript)
        terminal.send("!ask Initial question")
        assert MockService.model_requests[-1]["model"] == "second-model"
        assert MockService.model_requests[-1]["max_tokens"] == 1024
        assert MockService.model_headers[-1]["x-project"] == "example"
        assert "authorization" not in MockService.model_headers[-1]
        terminal.send("SELECT 1;")
        terminal.send("!ask Follow-up question")
        assert len(MockService.model_requests[-1]["messages"]) == 4
        terminal.send("!backend first")
        terminal.send("!ask Explain Q1")
        assert MockService.model_requests[-1]["model"] == "first-model"
        assert MockService.model_headers[-1]["authorization"] == "Bearer private-token"
        assert MockService.model_headers[-1]["x-custom-key"] == "private-header"
        assert len(MockService.model_requests[-1]["messages"]) == 2
        assert last_context()["query_records"][0]["id"] == "Q1"
        count = len(MockService.model_requests)
        terminal.send("!backend broken")
        assert len(MockService.model_requests) == count
        terminal.send("!ask Still on first?")
        assert MockService.model_requests[-1]["model"] == "first-model"
        assert len(MockService.model_requests[-1]["messages"]) == 4
        terminal.send("!backend env")
        terminal.send("!ask Explicit environment backend")
        assert MockService.model_requests[-1]["model"] == "mock"
        assert len(MockService.model_requests[-1]["messages"]) == 2
        assert last_context()["query_records"][0]["id"] == "Q1"
        terminal.send("!context clear")
        terminal.send("!ask After clearing")
        assert last_context()["query_records"] == []
        assert len(MockService.model_requests[-1]["messages"]) == 2
        terminal.finish()
        output = b"".join(terminal.transcript)
        for secret in [b"private-token", b"private-header", b"private-url-secret"]:
            assert secret not in output

    # Without a CLI override, the selected config wins over legacy environment.
    with Terminal(executable, env, port) as terminal:
        terminal.expect("smart> ")
        terminal.send("!ask Config default")
        assert MockService.model_requests[-1]["model"] == "first-model"
        terminal.finish()

    # Profile definitions alone never select an HTTP backend. Startup only
    # discovers executable paths; explicitly select the mock service before asking.
    write_config(env, config.replace('backend = "first"\n', "", 1))
    count = len(MockService.model_requests)
    with Terminal(executable, env, port) as terminal:
        terminal.expect("smart> ")
        assert len(MockService.model_requests) == count
        terminal.send("!backend second")
        terminal.send("!ask Explicit selection")
        assert MockService.model_requests[-1]["model"] == "second-model"
        terminal.finish()

    # Semantic and syntax errors disable AI, but the explicit SQL DSN still works.
    for invalid in [
        "[agent]\nbackend=123\nsecret='private-config-secret'",
        "[agent\nsecret='private-config-secret'",
    ]:
        write_config(env, invalid)
        prompt = f"root@127.0.0.1:{port}/default/default> " if invalid.startswith("[agent\n") else "smart> "
        with Terminal(executable, env, port, startup_prompt=prompt) as terminal:
            terminal.expect(prompt)
            count = len(MockService.model_requests)
            sql_count = len(MockService.sql_requests)
            terminal.send("!ask Bad configuration", prompt)
            terminal.send("!backend env", prompt)
            assert len(MockService.model_requests) == count
            terminal.send("SELECT 1;", prompt)
            assert len(MockService.sql_requests) == sql_count + 1
            assert b"private-config-secret" not in b"".join(terminal.transcript)
            terminal.finish()


def process_running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A killed orphan may briefly remain a zombie under a container's PID 1.
    stat = f"/proc/{pid}/stat"
    if os.path.exists(stat):
        try:
            with open(stat) as file:
                return file.read().rsplit(")", 1)[1].split()[0] != "Z"
        except (FileNotFoundError, ProcessLookupError):
            return False
    return True


def wait_stopped(record):
    pids = [pid for pid in [record["pid"], record["child"]] if pid]
    deadline = time.monotonic() + 5
    while any(process_running(pid) for pid in pids) or os.path.exists(record["cwd"]):
        if time.monotonic() >= deadline:
            for pid in pids:
                if process_running(pid):
                    os.kill(pid, signal.SIGKILL)
            raise AssertionError("CLI cleanup left a running process or temporary directory")
        time.sleep(0.05)
    assert not os.path.exists(record["cwd"]), "Temporary CLI directory leaked"


def cli_records(path):
    if not os.path.exists(path):
        return []
    with open(path) as file:
        return [json.loads(line) for line in file if line.strip()]


def wait_cli_record(path, count):
    deadline = time.monotonic() + 5
    while len(cli_records(path)) < count:
        if time.monotonic() > deadline:
            raise AssertionError("Mock CLI did not receive its question")
        time.sleep(0.02)
    return cli_records(path)[-1]


def exercise_cli_backends(executable, env, port):
    with tempfile.TemporaryDirectory(prefix="bendsql-mock-cli-") as directory:
        mock = os.path.join(directory, "mock-agent")
        source = os.path.join(os.path.dirname(__file__), "mock_agent_cli.py")
        with open(source) as file:
            contents = file.read()
        with open(mock, "w") as file:
            file.write(f"#!{sys.executable}\n{contents}")
        os.chmod(mock, 0o700)
        record_path = os.path.join(directory, "requests.jsonl")
        cli_env = dict(
            env, CLI_TEST_RECORD=record_path, UNRELATED_SECRET="private-unrelated-secret",
            BENDSQL_PASSWORD="private-db-password", DATABEND_DSN="private-db-dsn",
            OPENAI_API_KEY="private-codex-key", ANTHROPIC_API_KEY="private-claude-key",
        )
        config = f"""
[agent]
backend = "claude"
timeout_secs = 2
[agent.backends.claude]
type = "cli"
adapter = "claude-code"
command = {json.dumps(mock)}
model = "configured-claude-model"
allow_external_agent = true
env_allowlist = ["CLI_TEST_RECORD"]
[agent.backends.codex]
type = "cli"
adapter = "codex"
command = {json.dumps(mock)}
model = "configured-codex-model"
allow_external_agent = true
env_allowlist = ["CLI_TEST_RECORD"]
[agent.backends.no-consent]
type = "cli"
adapter = "codex"
command = {json.dumps(mock)}
[agent.backends.not-installed]
type = "cli"
adapter = "claude-code"
command = "bendsql-nonexistent-cli-91379"
allow_external_agent = true
"""
        write_config(cli_env, config)
        api_count = len(MockService.model_requests)
        with Terminal(executable, cli_env, port) as terminal:
            terminal.expect("smart> ")
            terminal.send("SELECT 1;")
            sql_count = len(MockService.sql_requests)
            for backend in ["claude", "codex"]:
                if backend == "codex":
                    terminal.send("!backend codex")
                count = len(cli_records(record_path))
                terminal.send("!ask Please explain Q1; do-not-put-this-in-argv")
                record = cli_records(record_path)[-1]
                assert len(cli_records(record_path)) == count + 1
                args = record["argv"]
                assert "do-not-put-this-in-argv" not in " ".join(args)
                assert "SELECT 1" not in " ".join(args)
                assert not any("dangerously" in arg for arg in args)
                expected_model = f"configured-{backend}-model"
                if backend == "claude":
                    assert args[args.index("--model") + 1] == expected_model
                    assert args[args.index("--tools") + 1] == ""
                    assert "--safe-mode" in args and "--no-session-persistence" in args
                    assert record["env"]["ANTHROPIC_API_KEY"] == "private-claude-key"
                    assert "OPENAI_API_KEY" not in record["env"]
                else:
                    assert "app-server" in args and "stdio://" in args
                    assert "--ignore-user-config" not in args and "--strict-config" not in args
                    assert record["model"] == expected_model
                    assert record["thread_settings"]["sandbox"] == "read-only"
                    assert record["thread_settings"]["ephemeral"] is True
                    assert record["controlling_terminal"] is False
                    assert 'approval_policy="never"' in args
                    assert record["env"]["OPENAI_API_KEY"] == "private-codex-key"
                    assert "ANTHROPIC_API_KEY" not in record["env"]
                for name in ["BENDSQL_PASSWORD", "DATABEND_DSN", "UNRELATED_SECRET"]:
                    assert name not in record["env"]
                assert record["cwd"] != os.getcwd() and not os.path.exists(record["cwd"])
                context = json.loads(record["messages"][-1]["content"])
                expected_id = "Q1" if backend == "claude" else "Q2"
                assert context["query_records"][0]["id"] == expected_id
                assert len(record["messages"]) == 2
                for question in ["exit-error", "bad-json", "failed-turn", "overflow-stdout"]:
                    terminal.send(f"!ask {question}")
                    assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("!ask After failed requests")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                before_output = len(b"".join(terminal.transcript))
                terminal.send("!ask overflow-stderr")
                assert b"AI> " in b"".join(terminal.transcript)[before_output:]
                if backend == "codex":
                    before_output = len(b"".join(terminal.transcript))
                    terminal.send("!ask future-metadata")
                    assert b"AI> " in b"".join(terminal.transcript)[before_output:]
                    for question in ["future-tool-item", "future-tool-request"]:
                        before_output = len(b"".join(terminal.transcript))
                        terminal.send(f"!ask {question}")
                        output = b"".join(terminal.transcript)[before_output:]
                        assert b"AI error:" in output and b"AI> " not in output
                terminal.send("!ask long-answer")
                assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
                terminal.send("!context clear")
                terminal.send("SELECT 1;")
                sql_count += 1
                # Cancel while the process and a descendant are holding both pipes.
                count = len(cli_records(record_path))
                os.write(terminal.fd, b"!ask block-cli\r")
                record = wait_cli_record(record_path, count + 1)
                os.write(terminal.fd, b"\x03")
                terminal.expect("AI request interrupted.")
                terminal.expect("smart> ")
                wait_stopped(record)
                if backend == "codex":
                    assert cli_records(record_path + ".interrupt")[-1]["pid"] == record["pid"]
                terminal.send("!ask After cancellation")
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                # Also exercise deadline-based cleanup, not only user cancellation.
                terminal.send("!ask block-cli")
                timed_out = cli_records(record_path)[-1]
                wait_stopped(timed_out)
                if backend == "codex":
                    assert cli_records(record_path + ".interrupt")[-1]["pid"] == timed_out["pid"]
                terminal.send("!ask After timeout")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                assert len(MockService.sql_requests) == sql_count
                assert len(MockService.model_requests) == api_count
            count = len(cli_records(record_path))
            terminal.send("!backend no-consent")
            terminal.send("!backend not-installed")
            assert len(cli_records(record_path)) == count
            terminal.send("!ask Still on codex")
            assert "app-server" in cli_records(record_path)[-1]["argv"]
            terminal.finish()
            output = b"".join(terminal.transcript)
            assert b"allow_external_agent = true" in output
            assert b"exceeded its byte budget" in output
            assert b"timed out" in output
            for secret in [b"private-cli-secret", b"private-db-password", b"private-db-dsn", b"private-unrelated-secret", b"private-codex-key", b"private-claude-key"]:
                assert secret not in output
            assert b"\x1b[31mMock CLI" not in output


def exercise_builtin_clis(executable, env, port):
    # Built-ins need no backend tables, but sharing requires explicit consent.
    write_config(env, "")
    source = os.path.join(os.path.dirname(__file__), "mock_agent_cli.py")
    with open(source) as file:
        contents = file.read()
    record_path = os.path.join(env["HOME"], "builtin_cli_requests.jsonl")
    api_count = len(MockService.model_requests)
    with tempfile.TemporaryDirectory(prefix="bendsql-builtin-bin-") as directory:
        for name in ["codex", "claude", "pi"]:
            path = os.path.join(directory, name)
            with open(path, "w") as file:
                file.write(f"#!{sys.executable}\n{contents}")
            os.chmod(path, 0o700)
        cli_env = dict(env, PATH=directory + os.pathsep + env.get("PATH", os.defpath),
                       GEMINI_API_KEY="private-pi-key")
        for name in list(cli_env):
            if name.startswith("BENDSQL_AGENT_"):
                del cli_env[name]
        for name in ["local-claude", "local-codex", "local-pi"]:
            with Terminal(executable, cli_env, port, backend=name) as terminal:
                terminal.expect("smart> ")
                terminal.send("!backend")
                output = b"".join(terminal.transcript)
                assert f"Current backend: {name}".encode() in output
                for builtin in [b"local-claude", b"local-codex", b"local-pi"]:
                    assert builtin in output
                terminal.send("SELECT 1;")
                sql_count = len(MockService.sql_requests)
                terminal.send("!ask Explain Q1 without backend configuration")
                record = cli_records(record_path)[-1]
                assert "--model" not in record["argv"]
                assert "CLI_TEST_RECORD" not in record["env"]
                assert len(record["messages"]) == 2
                assert json.loads(record["messages"][-1]["content"])["query_records"][0]["id"] == "Q1"
                if name == "local-codex":
                    assert "app-server" in record["argv"]
                    terminal.send("!backend claude-code")
                    terminal.send("!ask Switch to built-in Claude")
                    assert "--print" in cli_records(record_path)[-1]["argv"]
                elif name == "local-claude":
                    assert "--print" in record["argv"]
                    terminal.send("!backend codex")
                    terminal.send("!ask Switch to built-in Codex")
                    assert "app-server" in cli_records(record_path)[-1]["argv"]
                elif name == "local-pi":
                    assert "--no-tools" in record["argv"] and "--no-session" in record["argv"]
                    assert "--no-extensions" in record["argv"] and "--no-mcp" in record["argv"]
                    assert record["env"]["PI_OFFLINE"] == "1"
                    assert record["env"]["GEMINI_API_KEY"] == "private-pi-key"
                    terminal.send("!backend claude")
                    terminal.send("!ask Switch to built-in Claude")
                    assert "--print" in cli_records(record_path)[-1]["argv"]
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                assert len(MockService.sql_requests) == sql_count
                assert len(MockService.model_requests) == api_count
                terminal.finish()
        # Exercise failures/cancellation for each new adapter using its native wire format.
        write_config(cli_env, '[agent]\ntimeout_secs=2')
        for name in ["pi"]:
            with Terminal(executable, cli_env, port, backend=name) as terminal:
                terminal.expect("smart> ")
                terminal.send("SELECT 1;")
                sql_count = len(MockService.sql_requests)
                help_count = len(cli_records(record_path + ".help"))
                terminal.send("!ask Explain Q1")
                assert len(cli_records(record_path + ".help")) == help_count + 1
                for question in ["exit-error", "bad-json", "failed-turn", "overflow-stdout", "tool-policy"]:
                    terminal.send(f"!ask {question}")
                    assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("!ask After failures")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                before_output = len(b"".join(terminal.transcript))
                terminal.send("!ask overflow-stderr")
                assert b"AI> " in b"".join(terminal.transcript)[before_output:]
                terminal.send("!ask long-answer")
                assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
                assert len(cli_records(record_path + ".help")) == help_count + 1
                terminal.send("!context clear")
                count = len(cli_records(record_path))
                os.write(terminal.fd, b"!ask block-cli\r")
                record = wait_cli_record(record_path, count + 1)
                os.write(terminal.fd, b"\x03")
                terminal.expect("AI request interrupted.")
                terminal.expect("smart> ")
                wait_stopped(record)
                terminal.send("!ask After cancellation")
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                terminal.send("!ask block-cli")
                wait_stopped(cli_records(record_path)[-1])
                terminal.send("!ask After timeout")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                assert len(MockService.sql_requests) == sql_count
                assert len(cli_records(record_path + ".help")) == help_count + 2
                terminal.finish()
                for secret in [b"private-cli-secret", b"private-thinking", b"private-pi-key"]:
                    assert secret not in b"".join(terminal.transcript)
        # Old versions fail before receiving context. No downgrade and no service fallback.
        for name in ["pi", "claude", "codex"]:
            write_config(cli_env, f'[agent.backends.local-{name}]\nenv_allowlist=["CLI_TEST_OLD"]')
            old_env = dict(cli_env, CLI_TEST_OLD="1")
            with Terminal(executable, old_env, port, backend=name) as terminal:
                terminal.expect("smart> ")
                count = len(cli_records(record_path))
                terminal.send("!ask Context must not be delivered")
                assert len(cli_records(record_path)) == count
                assert b"Context was not sent" in b"".join(terminal.transcript)
                if name == "codex":
                    assert b"requires Codex 0.145.0 or newer" in b"".join(terminal.transcript)
                else:
                    assert cli_records(record_path + ".help")[-1]["stdin"] == ""
                terminal.send("SELECT 1;")
                terminal.finish()
        assert all(record["stdin"] == "" for record in cli_records(record_path + ".help"))
        assert len(MockService.model_requests) == api_count
        # Override just a model on a built-in profile; type/adapter/consent inherit.
        write_config(cli_env, '[agent]\nbackend="local-codex"\n[agent.backends.local-codex]\nmodel="custom-builtin-model"')
        with Terminal(executable, cli_env, port, backend="codex") as terminal:
            terminal.expect("smart> ")
            terminal.send("!ask Built-in model override")
            assert cli_records(record_path)[-1]["model"] == "custom-builtin-model"
            terminal.finish()
        # A disabled canonical profile cannot be bypassed through its alias.
        write_config(cli_env, '[agent.backends.local-codex]\nallow_external_agent=false')
        with Terminal(executable, cli_env, port, backend="codex") as terminal:
            terminal.expect("smart> ")
            count = len(cli_records(record_path))
            terminal.send("!ask Disabled built-in")
            assert len(cli_records(record_path)) == count
            terminal.send("SELECT 1;")
            terminal.finish()


def acp_records(path, phase):
    return [record for record in cli_records(path) if record["phase"] == phase]


def wait_acp_prompt(path, count):
    deadline = time.monotonic() + 5
    while len(acp_records(path, "prompt")) < count:
        if time.monotonic() > deadline:
            raise AssertionError("Mock ACP adapter did not receive its question")
        time.sleep(0.02)
    return acp_records(path, "prompt")[-1]


def exercise_acp_backend(executable, env, port):
    with tempfile.TemporaryDirectory(prefix="bendsql-mock-acp-") as directory:
        mock = os.path.join(directory, "mock-acp")
        source = os.path.join(os.path.dirname(__file__), "mock_agent_acp.py")
        with open(source) as file:
            contents = file.read()
        with open(mock, "w") as file:
            file.write(f"#!{sys.executable}\n{contents}")
        os.chmod(mock, 0o700)
        path = os.path.join(directory, "requests.jsonl")
        acp_env = dict(env, ACP_TEST_RECORD=path, CUSTOM_ACP_AUTH="private-acp-auth",
                       ACP_TEST_BAD_VERSION="1", ACP_TEST_HANG_INIT="1",
                       OPENAI_API_KEY="not-inherited", ANTHROPIC_API_KEY="not-inherited",
                       BENDSQL_PASSWORD="private-db-password", DATABEND_DSN="private-db-dsn")
        config = f"""
[agent]
backend = "mock-acp"
timeout_secs = 2
[agent.backends.mock-acp]
type = "acp"
command = {json.dumps(mock)}
args = ["--stdio", "value with spaces"]
allow_external_agent = true
env_allowlist = ["ACP_TEST_RECORD", "CUSTOM_ACP_AUTH"]
[agent.backends.bad-version]
type = "acp"
command = {json.dumps(mock)}
allow_external_agent = true
env_allowlist = ["ACP_TEST_RECORD", "ACP_TEST_BAD_VERSION"]
[agent.backends.hang-init]
type = "acp"
command = {json.dumps(mock)}
allow_external_agent = true
env_allowlist = ["ACP_TEST_RECORD", "ACP_TEST_HANG_INIT"]
[agent.backends.no-consent]
type = "acp"
command = {json.dumps(mock)}
"""
        write_config(acp_env, config)
        api_count = len(MockService.model_requests)
        with Terminal(executable, acp_env, port) as terminal:
            terminal.expect("smart> ")
            assert not cli_records(path), "Adapter must not start until a question"
            terminal.send("SELECT 1;")
            sql_count = len(MockService.sql_requests)
            terminal.send("!ask Explain Q1; do-not-put-this-in-argv")
            assert b"Mock ACP answer" in b"".join(terminal.transcript)
            prompt = acp_records(path, "prompt")[-1]
            init = acp_records(path, "initialize")[-1]
            session = acp_records(path, "new_session")[-1]
            assert init["params"]["protocolVersion"] == 1
            capabilities = init["params"]["clientCapabilities"]
            assert not capabilities.get("terminal", False)
            assert not capabilities.get("fs", {}).get("readTextFile", False)
            assert not capabilities.get("fs", {}).get("writeTextFile", False)
            assert session["params"]["mcpServers"] == []
            assert session["params"]["cwd"] == prompt["cwd"]
            assert init["argv"] == ["--stdio", "value with spaces"]
            assert "do-not-put-this-in-argv" not in " ".join(init["argv"])
            assert init["env"]["CUSTOM_ACP_AUTH"] == "private-acp-auth"
            for key in ["BENDSQL_PASSWORD", "DATABEND_DSN", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ACP_TEST_BAD_VERSION"]:
                assert key not in init["env"]
            assert json.loads(prompt["messages"][-1]["content"])["query_records"][0]["id"] == "Q1"
            wait_stopped(prompt)
            terminal.send("!ask Follow-up")
            prompt = acp_records(path, "prompt")[-1]
            assert len(prompt["messages"]) == 4
            wait_stopped(prompt)
            failures = ["rpc-error", "bad-json", "wrong-session", "malformed-update", "unsupported-content",
                        "overflow-frame", "overflow-wire", "tool-call", "permission",
                        "read-file", "write-file", "terminal", "extension", "failed-turn", "no-answer"]
            for question in failures:
                terminal.send(f"!ask {question}")
                prompt = acp_records(path, "prompt")[-1]
                assert len(prompt["messages"]) == 6
                wait_stopped(prompt)
            replies = acp_records(path, "client_reply")
            assert any(reply["reply"].get("result", {}).get("outcome", {}).get("outcome") == "cancelled" for reply in replies)
            assert sum(reply["reply"].get("error", {}).get("code") == -32601 for reply in replies) >= 4
            terminal.send("!ask After errors")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 6
            terminal.send("!ask long-answer")
            assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
            terminal.send("!ask max-tokens")
            assert b"[ACP answer truncated by model output limit]" in b"".join(terminal.transcript)
            terminal.send("!context clear")
            count = len(acp_records(path, "prompt"))
            os.write(terminal.fd, b"!ask block-acp\r")
            prompt = wait_acp_prompt(path, count + 1)
            os.write(terminal.fd, b"\x03")
            terminal.expect("AI request interrupted.")
            terminal.expect("smart> ")
            wait_stopped(prompt)
            assert acp_records(path, "cancel")[-1]["pid"] == prompt["pid"]
            terminal.send("!ask After cancellation")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 2
            terminal.send("!ask block-acp")
            prompt = acp_records(path, "prompt")[-1]
            wait_stopped(prompt)
            assert acp_records(path, "cancel")[-1]["pid"] == prompt["pid"]
            terminal.send("!ask After timeout")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 4
            terminal.send("!ask ignore-cancel")
            wait_stopped(acp_records(path, "prompt")[-1])
            # Initialization failure/timeout must never receive a prompt/context.
            for backend in ["bad-version", "hang-init"]:
                count = len(acp_records(path, "prompt"))
                terminal.send(f"!backend {backend}")
                terminal.send("!ask Context must not be delivered")
                assert len(acp_records(path, "prompt")) == count
                init = acp_records(path, "initialize")[-1]
                wait_stopped(dict(init, child=None))
            terminal.send("!backend no-consent")
            assert len(MockService.model_requests) == api_count
            assert len(MockService.sql_requests) == sql_count
            terminal.send("SELECT 1;")
            terminal.finish()
            output = b"".join(terminal.transcript)
            for secret in [b"private-acp-secret", b"private-thinking", b"private-acp-auth", b"private-db-password", b"private-db-dsn", b"not-inherited"]:
                assert secret not in output
            assert b"\x1b[31mMock ACP" not in output
        # ACP message chunks are drawn live before the turn completes.
        with Terminal(executable, dict(acp_env, TERM="xterm"), port, size=(24, 80)) as terminal:
            terminal.expect("smart> ")
            os.write(terminal.fd, b"!ask stream-acp\r")
            terminal.expect("ACP streamed first")
            terminal.expect("smart> ")
            output = b"".join(terminal.transcript)
            assert output.count(b"ACP streamed first") >= 2, "A partial frame must precede the final answer"
            screen = visible_terminal(output)
            assert screen.count("ACP streamed first and second") == 1, screen
            wait_stopped(acp_records(path, "prompt")[-1])
            terminal.finish()


def exercise_smart_input_boundaries(executable, env, port):
    write_config(env, '[agent]\nbackend="env"')
    with Terminal(executable, env, port) as terminal:
        terminal.expect("smart> ")
        sql_count = len(MockService.sql_requests)
        api_count = len(MockService.model_requests)
        for text in ["SELECT 1; Explain the result", "DROP TABLE sensitive; Why would this happen?",
                     "/* example */ SELECT 1; explain", "SELECT 1; SELECT * FROM",
                     "INVALID SQL;", "SELECT * FROM;", "GET file://result @stage"]:
            terminal.send(text)
        assert len(MockService.sql_requests) == sql_count
        assert len(MockService.model_requests) == api_count
        for comment in ["-- a complete comment", "/* a closed comment */"]:
            terminal.send(comment)
        terminal.send("/* unfinished", "smart(sql)> ")
        terminal.send("comment */")
        assert len(MockService.sql_requests) == sql_count
        terminal.send("SELECT", "smart(sql)> ")
        terminal.send("1; please explain", "smart(sql)> ")
        assert len(MockService.sql_requests) == sql_count
        terminal.send("1;")
        assert len(MockService.sql_requests) == sql_count + 1
        terminal.send("SELECT 2; SELECT ';why?'; -- comment")
        assert len(MockService.sql_requests) == sql_count + 3
        terminal.send("What does the previous result mean?")
        assert len(MockService.model_requests) == api_count + 1
        terminal.send("!set sql_delimiter |")
        terminal.send("SELECT 3| Explain this")
        assert len(MockService.sql_requests) == sql_count + 3
        terminal.send("SELECT 'a|b'| SELECT 4|")
        assert len(MockService.sql_requests) == sql_count + 5
        terminal.send("!sql SELECT 5|")
        assert len(MockService.sql_requests) == sql_count + 6
        terminal.send("!set multi_line false")
        terminal.send("SELECT * FROM")
        assert len(MockService.sql_requests) == sql_count + 6
        terminal.send("SELECT 6")
        assert len(MockService.sql_requests) == sql_count + 7
        terminal.send("!set multi_line true")
        terminal.send("!set sql_delimiter ;")
        terminal.send("SELECT 7", "smart(sql)> ")
        terminal.send(";")
        assert len(MockService.sql_requests) == sql_count + 8
        terminal.send("!mode sql", "sql> ")
        terminal.send("Not SQL;", "sql> ")
        assert len(MockService.sql_requests) == sql_count + 9
        terminal.send("!mode agent", "agent> ")
        terminal.send("SELECT 8;", "agent> ")
        assert len(MockService.sql_requests) == sql_count + 9
        assert len(MockService.model_requests) == api_count + 2
        terminal.finish()


def wait_http_count(items, count):
    deadline = time.monotonic() + 5
    while len(items) < count:
        if time.monotonic() > deadline:
            raise AssertionError("Mock SQL request did not start")
        time.sleep(0.02)


def exercise_sql_cancellation(executable, env, port):
    write_config(env, '[agent]\nbackend="env"')
    MockService.release_queries.clear()
    try:
        with Terminal(executable, env, port) as terminal:
            terminal.expect("smart> ")
            terminal.send("SELECT 1;")
            sql_count = len(MockService.sql_requests)
            kills = len(MockService.kill_requests)
            os.write(terminal.fd, b"SELECT BLOCK_SUBMISSION; SELECT NEVER_AFTER_CANCEL;\r")
            wait_http_count(MockService.sql_requests, sql_count + 1)
            start = time.monotonic()
            os.write(terminal.fd, b"\x03")
            terminal.expect("smart> ")
            assert time.monotonic() - start < 2, "Submission cancellation waited for a response"
            assert len(MockService.sql_requests) == sql_count + 1
            assert len(MockService.kill_requests) == kills, "A previous query was incorrectly cancelled"
            terminal.send("!ask Explain Q2")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q2")
            assert record["status"] == "interrupted" and record["query_id"] is None
            assert record["cancel_request_sent"] is False
            assert "state and Query ID may be unknown" in record["error"]
            MockService.release_queries.set()
            terminal.send("SELECT 2;")
            terminal.send("!sql SELECT SPOOF_INTERRUPTED;")
            assert len(MockService.kill_requests) == kills
            terminal.send("!ask Explain Q4")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q4")
            assert record["status"] == "failed" and record["cancel_request_sent"] is False
            MockService.release_queries.clear()
            pages = len(MockService.page_requests)
            os.write(terminal.fd, b"SELECT BLOCK_PAGE;\r")
            wait_http_count(MockService.page_requests, pages + 1)
            os.write(terminal.fd, b"\x03")
            terminal.expect("smart> ")
            assert len(MockService.kill_requests) == kills + 1
            terminal.send("!ask Explain Q5")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q5")
            assert record["status"] == "interrupted" and record["query_id"] is not None
            assert record["cancel_request_sent"] is True
            assert MockService.kill_requests[-1] == f'/v1/query/{record["query_id"]}/kill'
            assert record["preview"]["complete"] is False
            MockService.release_queries.clear()
            pages = len(MockService.page_requests)
            os.write(terminal.fd, b"SELECT BLOCK_PAGE_STALL_KILL;\r")
            wait_http_count(MockService.page_requests, pages + 1)
            start = time.monotonic()
            os.write(terminal.fd, b"\x03")
            terminal.expect("smart> ")
            assert time.monotonic() - start < 5, "Cancellation request has no deadline"
            assert len(MockService.kill_requests) == kills + 2
            terminal.send("!ask Explain Q6")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q6")
            assert record["status"] == "interrupted" and record["cancel_request_sent"] is False
            assert b"Cancellation request timed out" in b"".join(terminal.transcript)
            MockService.release_queries.set()
            terminal.finish()
    finally:
        MockService.release_queries.set()


def exercise_default_backend_discovery(executable, env, port):
    with tempfile.TemporaryDirectory(prefix="bendsql-default-bin-") as directory:
        source = os.path.join(os.path.dirname(__file__), "mock_agent_cli.py")
        with open(source) as file:
            script = f"#!{sys.executable}\n{file.read()}"
        # An isolated PATH ensures missing candidates never run real installed CLIs.
        auto_env = dict(env, PATH=directory)
        path = os.path.join(env["HOME"], "builtin_cli_requests.jsonl")
        api_count = len(MockService.model_requests)
        for programs, expected, marker in [
            (["codex", "claude", "pi"], "local-codex", "app-server"),
            (["claude", "pi"], "local-claude", "--no-session-persistence"),
            (["pi"], "local-pi", "--no-tools"),
            ([], None, None),
        ]:
            for name in ["codex", "claude", "pi"]:
                filename = os.path.join(directory, name)
                if os.path.exists(filename):
                    os.unlink(filename)
                if name in programs:
                    with open(filename, "w") as file:
                        file.write(script)
                    os.chmod(filename, 0o700)
            write_config(auto_env, "")
            count = len(cli_records(path))
            with Terminal(executable, auto_env, port) as terminal:
                terminal.expect("smart> ")
                assert len(cli_records(path)) == count, "Discovery must not start a model"
                terminal.send("!backend")
                if expected:
                    assert f"Current backend: {expected}".encode() in b"".join(terminal.transcript)
                terminal.send("!mode agent", "agent> ")
                terminal.send("hello, how are you", "agent> ")
                if expected:
                    assert len(cli_records(path)) == count + 1
                    assert marker in cli_records(path)[-1]["argv"]
                    terminal.send("exit-error", "agent> ")
                    assert len(cli_records(path)) == count + 2, "A failed request must not try another CLI"
                    assert marker in cli_records(path)[-1]["argv"]
                    terminal.send("hello again", "agent> ")
                    assert marker in cli_records(path)[-1]["argv"]
                    # Explicit selection beats auto detection, even when unavailable.
                    before = len(cli_records(path))
                    terminal.send("!backend env", "agent> ")
                    terminal.send("hello using explicit env", "agent> ")
                    assert len(cli_records(path)) == before
                    assert len(MockService.model_requests) == api_count + 1
                    api_count += 1
                else:
                    assert len(cli_records(path)) == count
                    assert b"No local coding CLI found" in b"".join(terminal.transcript)
                    assert b"Set BENDSQL_AGENT_BASE_URL" not in b"".join(terminal.transcript)
                assert len(MockService.model_requests) == api_count
                os.write(terminal.fd, b"!help\r")
                terminal.expect("Use !context clear before discussing unrelated or sensitive data.")
                terminal.expect("agent> ")
                assert b"!mode [sql|smart|agent]" in b"".join(terminal.transcript)
                terminal.send("!mode sql", "sql> ")
                terminal.send("SELECT 1;", "sql> ")
                terminal.finish()
        # Merely defining profiles does not pick arbitrary HTTP services. An
        # explicit config selection still wins over installed local executables.
        with open(os.path.join(directory, "codex"), "w") as file:
            file.write(script)
        os.chmod(os.path.join(directory, "codex"), 0o700)
        write_config(auto_env, '[agent]\nbackend="env"')
        before = len(cli_records(path))
        with Terminal(executable, auto_env, port) as terminal:
            terminal.expect("smart> ")
            terminal.send("!ask Config explicitly selected env")
            assert len(cli_records(path)) == before
            assert len(MockService.model_requests) == api_count + 1
            api_count += 1
            terminal.finish()
        with Terminal(executable, auto_env, port, backend="missing-profile") as terminal:
            terminal.expect("smart> ")
            terminal.send("!ask Explicit invalid selection must not discover codex")
            assert len(cli_records(path)) == before
            assert len(MockService.model_requests) == api_count
            terminal.finish()


def visible_terminal(transcript):
    # Minimal screen reconstruction for the CSI cursor/erase sequences emitted
    # by rustyline and indicatif. Raw PTY transcripts legitimately contain ticks;
    # the persistent rendered terminal must not contain a spinner after completion.
    import re
    text = transcript.decode(errors="replace")
    rows, row, column = [[]], 0, 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\x1b":
            match = re.match(r"\x1b\[([0-9;?]*)([A-Za-z~])", text[index:])
            if match:
                parameters, action = match.groups()
                values = [int(value or "0") for value in parameters.lstrip("?").split(";")]
                count = values[0] or 1
                if action == "A": row = max(0, row - count)
                elif action in ["B", "E"]:
                    row += count
                    if action == "E": column = 0
                elif action == "F": row, column = max(0, row - count), 0
                elif action == "C": column += count
                elif action == "D": column = max(0, column - count)
                elif action == "G": column = count - 1
                elif action in ["H", "f"]:
                    row, column = count - 1, (values[1] if len(values) > 1 else 1) - 1
                elif action == "K":
                    while len(rows) <= row: rows.append([])
                    if values[0] == 2: rows[row] = []
                    elif values[0] == 0: del rows[row][column:]
                    else:
                        for position in range(min(column + 1, len(rows[row]))): rows[row][position] = " "
                elif action == "J" and values[0] in [2, 3]: rows = [[]]; row = column = 0
                elif action == "J" and values[0] == 0:
                    if row < len(rows):
                        del rows[row][column:]
                        del rows[row + 1:]
                index += len(match.group())
                continue
            index += 2
            continue
        if char == "\r": column = 0
        elif char == "\n": row += 1
        elif char == "\b": column = max(0, column - 1)
        elif not char.isprintable(): pass
        else:
            while len(rows) <= row: rows.append([])
            while len(rows[row]) <= column: rows[row].append(" ")
            rows[row][column] = char
            column += 1
        index += 1
    return "\n".join("".join(line).rstrip() for line in rows)


def exercise_transient_ai_spinner(executable, env, port):
    write_config(env, '[agent]\nbackend="env"\ntimeout_secs=1')
    animated_env = dict(env, TERM="xterm")
    MockService.release_models.clear()
    try:
        with Terminal(executable, animated_env, port) as terminal:
            terminal.expect("smart> ")
            # Do not use send(): readline's input redraws also contain the prompt.
            for question, output in [("slow-spinner", "AI> "), ("error-spinner", "AI error: ")]:
                before = len(b"".join(terminal.transcript))
                os.write(terminal.fd, question.encode() + b"\r")
                terminal.expect("AI thinking")
                terminal.expect(output)
                terminal.expect("smart> ")
                transcript = b"".join(terminal.transcript)
                assert transcript[before:].count(b"AI thinking") >= 2, "Spinner must animate during a slow request"
                assert "AI thinking" not in visible_terminal(transcript), "Spinner survived final output"
            count = len(MockService.model_requests)
            os.write(terminal.fd, b"wait-spinner\r")
            terminal.expect("AI thinking")
            wait_http_count(MockService.model_requests, count + 1)
            os.write(terminal.fd, b"\x03")
            terminal.expect("AI request interrupted.")
            terminal.expect("smart> ")
            assert "AI thinking" not in visible_terminal(b"".join(terminal.transcript))
            MockService.release_models.set()
            MockService.release_models.clear()
            os.write(terminal.fd, b"wait-spinner\r")
            terminal.expect("AI thinking")
            terminal.expect("AI error: ")
            terminal.expect("smart> ")
            assert "AI thinking" not in visible_terminal(b"".join(terminal.transcript))
            MockService.release_models.set()
            os.write(terminal.fd, b"slow-spinner\r")
            terminal.expect("AI> ")
            terminal.expect("smart> ")
            screen = visible_terminal(b"".join(terminal.transcript))
            assert "Mock answer grounded in cached records." in screen
            assert "AI thinking" not in screen
            terminal.finish()
    finally:
        MockService.release_models.set()


def exercise_default_sql_modes_and_embedded_skills(executable, env, port):
    import re
    write_config(env, '[agent]\nbackend="env"')
    sql_count, ai_count = len(MockService.sql_requests), len(MockService.model_requests)
    animated = dict(env, TERM="xterm")
    with Terminal(executable, animated, port, mode=None, autocomplete=True) as terminal:
        terminal.expect("sql> ")
        assert len(MockService.sql_requests) == sql_count + 2  # Version + SQL completion catalog.
        assert len(MockService.model_requests) == ai_count
        os.write(terminal.fd, b"!mode\r")
        terminal.expect("Options: sql | smart | agent")
        terminal.expect("sql> ")
        os.write(terminal.fd, b"!mode sm\t\r")
        terminal.expect("Switched to smart mode.")
        terminal.expect("smart> ")
        os.write(terminal.fd, b"!mode ag\t\r")
        terminal.expect("Switched to agent mode.")
        terminal.expect("agent> ")
        for question in ["sel", "autocomplete_table_"]:
            os.write(terminal.fd, question.encode())
            terminal.expect(question)
            os.write(terminal.fd, b"\t\x03")
            terminal.expect("agent> ")
        assert len(MockService.model_requests) == ai_count
        os.write(terminal.fd, b"!mode smart\r")
        terminal.expect("smart> ")
        before = len(b"".join(terminal.transcript))
        os.write(terminal.fd, b"autocomplete_table_")
        terminal.expect("autocomplete_table_")
        os.write(terminal.fd, b"\t\x03")
        terminal.expect("smart> ")
        assert b"unique" not in b"".join(terminal.transcript)[before:]
        os.write(terminal.fd, b"!mode sql\r")
        terminal.expect("Switched to sql mode.")
        terminal.expect("sql> ")
        assert len(MockService.sql_requests) == sql_count + 2, "Catalog should not reload on every switch"
        os.write(terminal.fd, b"autocomplete_table_")
        terminal.expect("unique")
        os.write(terminal.fd, b"\x03")
        terminal.expect("\x1b[?2004l")  # The old readline ended; ignore input redraw prompts.
        terminal.expect("sql> ")
        before = len(b"".join(terminal.transcript))
        os.write(terminal.fd, b"!mode agent\r")
        terminal.expect("Switched to agent mode.")
        terminal.expect("agent> ")
        os.write(terminal.fd, b"!sql SELECT 'private-query-value';\r")
        terminal.expect("[Q1] Query context cached")
        terminal.expect("agent> ")
        os.write(terminal.fd, b"!context\r")
        terminal.expect("Q1: success")
        terminal.expect("agent> ")
        os.write(terminal.fd, b"!ask Explain Q1\r")
        terminal.expect("AI> ")
        terminal.expect("agent> ")
        assert last_context()["query_records"][0]["id"] == "Q1"
        assert last_context()["query_records"][0]["sql"] == "SELECT 'private-query-value'"
        system = MockService.model_requests[-1]["messages"][0]["content"]
        assert "BendSQL embedded skills" in system and "databend/SKILL.md" in system
        assert "Directory:" not in system and "### README.md" not in system
        assert "private-query-value" not in system
        assert "supplied inline" in system
        os.write(terminal.fd, b"!context clear\r")
        terminal.expect("Query memory and conversation cleared.")
        terminal.expect("agent> ")
        os.write(terminal.fd, b"!ask Hello after clear\r")
        terminal.expect("AI> ")
        terminal.expect("agent> ")
        assert last_context()["query_records"] == []
        assert "databend/SKILL.md" in MockService.model_requests[-1]["messages"][0]["content"]
        terminal.finish()
    # No SQL catalog request in smart/agent startup. It is fetched lazily once
    # when the user explicitly enters SQL mode, without enabling hints elsewhere.
    for mode in ["smart", "agent"]:
        count = len(MockService.sql_requests)
        with Terminal(executable, env, port, mode=mode, autocomplete=True) as terminal:
            terminal.expect(f"{mode}> ")
            assert len(MockService.sql_requests) == count + 1
            terminal.send("!mode sql", "sql> ")
            assert len(MockService.sql_requests) == count + 2
            terminal.send(f"!mode {mode}", f"{mode}> ")
            terminal.send("!mode sql", "sql> ")
            assert len(MockService.sql_requests) == count + 2
            terminal.finish()
    # Batch SQL remains non-AI even when interactive modes are available.
    dsn = f"databend://root:@127.0.0.1:{port}/default?sslmode=disable&login=disable"
    with tempfile.TemporaryDirectory(prefix="bendsql-batch-home-") as home:
        batch_env = dict(env, HOME=home)
        before = len(MockService.model_requests)
        result = subprocess.run([executable, "--dsn", dsn], input=b"SELECT 1;\n", env=batch_env, capture_output=True, timeout=15)
        assert result.returncode == 0 and b"sql> " not in result.stdout
        assert len(MockService.model_requests) == before
        result = subprocess.run([executable, "--dsn", dsn, "--query=SELECT 2;"], env=batch_env, capture_output=True, timeout=15)
        assert result.returncode == 0 and len(MockService.model_requests) == before
        result = subprocess.run([executable, "--agent"], env=batch_env, capture_output=True, timeout=15)
        assert result.returncode != 0 and b"unexpected argument '--agent'" in result.stderr
    # Removed command prefixes are not compatibility aliases.
    with Terminal(executable, env, port, mode=None) as terminal:
        terminal.expect("sql> ")
        os.write(terminal.fd, b"!backend amp\r")
        terminal.expect("Backend unchanged:")
        terminal.expect("sql> ")
        before = len(b"".join(terminal.transcript))
        os.write(terminal.fd, b"/mode smart;\r")
        terminal.expect("sql> ")
        assert b"Query context cached" not in b"".join(terminal.transcript)[before:]
        os.write(terminal.fd, b"!context\r")
        terminal.expect("No cached queries.")
        terminal.expect("sql> ")
        terminal.finish()


def exercise_prompt_templates(executable, env, port):
    # Complete configured templates win over a hard-coded mode prefix. Cases
    # intentionally include one with no trailing space and literal braces.
    for template, visible in [
        ("🐳 @{database}:) ", "🐳 @default:) "),
        ("🐳 @{database} [{mode}]:) ", None),
        ("{user}@{host}:{port}/{catalog}/{database} [{mode}]> ", None),
        ("🐳 @{database}:)", "🐳 @default:)"),
        ("literal {unknown} [{mode}]> ", None),
    ]:
        write_config(env, f'[settings]\nprompt = {json.dumps(template, ensure_ascii=False)}\n[agent]\nbackend="env"\n')
        def expected(mode):
            return visible if visible is not None else template.replace("{mode}", mode).replace("{user}", "root").replace("{host}", "127.0.0.1").replace("{port}", str(port)).replace("{catalog}", "default").replace("{database}", "default")
        initial = expected("sql")
        with Terminal(executable, env, port, mode=None, startup_prompt=initial) as terminal:
            terminal.expect(initial)
            for mode in ["smart", "agent", "sql"]:
                os.write(terminal.fd, f"!mode {mode}\r".encode())
                terminal.expect(f"Switched to {mode} mode.")
                terminal.expect(expected(mode))
            # Changing the template live also updates mode rendering immediately.
            terminal.send("!set prompt [{mode}]@{database}", "[sql]@default")
            os.write(terminal.fd, b"!mode smart\r")
            terminal.expect("Switched to smart mode.")
            terminal.expect("[smart]@default")
            terminal.send("SELECT", "smart(sql)> ")
            terminal.send("1;", "[smart]@default")
            os.write(terminal.fd, b"!mode sql\r")
            terminal.expect("Switched to sql mode.")
            terminal.expect("[sql]@default")
            terminal.finish()
            output = b"".join(terminal.transcript)
            for mode in ["sql", "smart", "agent"]:
                assert (f"{mode}> 🐳").encode() not in output
    write_config(env, "")


def exercise_openai_provider(executable, env, port):
    env = dict(env, MOCK_OPENAI_KEY="private-provider-key", MOCK_HEADER_KEY="private-header-key")
    write_config(env, f'''[agent]
backend = "mock-openai"
timeout_secs = 2
[agent.backends.mock-openai]
type = "openai-compatible"
base_url = "http://127.0.0.1:{port}/v1/"
model = "deployment-not-a-known-model"
api_key_env = "MOCK_OPENAI_KEY"
header_envs = {{ "X-API-Key" = "MOCK_HEADER_KEY" }}
max_tokens = 1024
''')
    with Terminal(executable, env, port, mode="agent") as terminal:
        terminal.expect("agent> ")
        terminal.send("!sql SELECT 1;", "agent> ")
        queries = len(MockService.sql_requests)
        start = len(MockService.model_requests)
        terminal.send("provider-success", "agent> ")
        request = MockService.model_requests[-1]
        assert len(MockService.model_requests) == start + 1
        assert request["model"] == "deployment-not-a-known-model"
        assert request["stream"] is False and request["max_tokens"] == 1024
        assert MockService.model_headers[-1]["authorization"] == "Bearer private-provider-key"
        assert MockService.model_headers[-1]["x-api-key"] == "private-header-key"
        assert [m["role"] for m in request["messages"]] == ["system", "user"]
        system = request["messages"][0]["content"]
        for reference in ["databend/SKILL.md", "https://docs.databend.com/", "EXPLAIN <statement>",
                          "EXPLAIN ANALYZE <statement>", "EXPLAIN ANALYZE GRAPHICAL <statement>", "Not a dry run"]:
            assert reference in system
        assert "Document excerpt truncated" not in system
        context = last_context()
        assert context["query_records"][0]["id"] == "Q1"
        assert context["query_records"][0]["preview"]["complete"] is False
        terminal.send("provider-follow-up", "agent> ")
        assert len(MockService.model_requests[-1]["messages"]) == 4
        for question, message in [
            ("provider-unauthorized", "HTTP 401"), ("provider-rate-limit", "HTTP 429"),
            ("provider-unavailable", "HTTP 503"), ("provider-invalid-json", "invalid chat completion response"),
            ("provider-empty-choices", "no text answer"), ("provider-no-text", "no text answer"),
            ("provider-oversized", "128 KiB limit"),
        ]:
            before = len(MockService.model_requests)
            os.write(terminal.fd, question.encode() + b"\r")
            terminal.expect(message)
            terminal.expect("agent> ")
            assert len(MockService.model_requests) == before + 1, "No implicit retries/provider fallback"
            assert len(MockService.model_requests[-1]["messages"]) == 6, "Failures must not enter dialogue"
        terminal.send("provider-recovered", "agent> ")
        assert len(MockService.model_requests[-1]["messages"]) == 6
        terminal.send("provider-suggest-sql", "agent> ")
        assert len(MockService.sql_requests) == queries, "Suggested SQL must never be executed"
        assert b"AI> DROP TABLE orders;" in b"".join(terminal.transcript)
        terminal.send("provider-controls", "agent> ")
        assert b"AI> [31mPrintable answer" in b"".join(terminal.transcript)
        assert b"AI> \x1b[31m" not in b"".join(terminal.transcript)
        terminal.send("provider-long-answer", "agent> ")
        assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
        terminal.send("!context clear", "agent> ")
        terminal.send("provider-after-clear", "agent> ")
        assert len(MockService.model_requests[-1]["messages"]) == 2
        assert last_context()["query_records"] == []
        assert "databend/SKILL.md" in MockService.model_requests[-1]["messages"][0]["content"]
        for secret in [b"private-provider-diagnostic", b"private-provider-key", b"private-header-key"]:
            assert secret not in b"".join(terminal.transcript)
        terminal.finish()


def exercise_ai_markdown_output(executable, env, port):
    import re
    write_config(env, '[agent]\nbackend="env"')
    colored = dict(env, TERM="xterm")
    with Terminal(executable, colored, port, mode="agent") as terminal:
        terminal.expect("agent> ")
        queries = len(MockService.sql_requests)
        os.write(terminal.fd, b"provider-markdown\r")
        terminal.expect("AI>")
        terminal.expect("agent> ")
        output = b"".join(terminal.transcript)
        display = re.sub(rb"\x1b\[[0-9;]*m", b"", output)
        assert b"AI>\r\nQuery analysis" in display
        assert b"Estimated rows are not actual timings." in display
        assert b"1. Inspect EXPLAIN." in display
        assert b"SELECT 'a|b',\r\n  42;" in display
        assert b"Docs (https://docs.databend.com/)" in display
        assert b"\x1b[1;36mQuery analysis" in output
        for hidden in [b"## Query analysis", b"**Estimated**", b"```sql", b"private-html-payload", b"private.example/image", b"AI thinking"]:
            assert hidden not in visible_terminal(output).encode()
        assert len(MockService.sql_requests) == queries
        terminal.send("provider-follow-up", "agent> ")
        history = MockService.model_requests[-1]["messages"]
        raw = next(m["content"] for m in history if m["role"] == "assistant")
        assert raw.startswith("## Query analysis") and "```sql" in raw and "**Estimated**" in raw
        assert "\x1b[1;36m" not in raw, "Generated styles must not enter conversation history"
        terminal.send("!set ai_output plain", "agent> ")
        before = len(b"".join(terminal.transcript))
        terminal.send("provider-markdown", "agent> ")
        plain = b"".join(terminal.transcript)[before:]
        assert b"AI> ## Query analysis" in plain and b"```sql" in plain
        assert b"\x1b[1;36mQuery analysis" not in plain
        terminal.send("!set ai_output markdown", "agent> ")
        before = len(b"".join(terminal.transcript))
        terminal.send("provider-markdown", "agent> ")
        assert b"\x1b[1;36mQuery analysis" in b"".join(terminal.transcript)[before:]
        terminal.send("!set ai_output html", "agent> ")
        assert b"AI output must be markdown or plain" in b"".join(terminal.transcript)
        assert len(MockService.sql_requests) == queries
        terminal.finish()
    # File config and explicit NO_COLOR both work without changing SQL output.
    write_config(env, '[settings]\nai_output="plain"\n[agent]\nbackend="env"')
    with Terminal(executable, colored, port, mode="agent") as terminal:
        terminal.expect("agent> ")
        terminal.send("provider-markdown", "agent> ")
        assert b"AI> ## Query analysis" in b"".join(terminal.transcript)
        terminal.finish()
    write_config(env, '[agent]\nbackend="env"')
    with Terminal(executable, dict(colored, NO_COLOR="1"), port, mode="agent") as terminal:
        terminal.expect("agent> ")
        terminal.send("provider-markdown", "agent> ")
        output = b"".join(terminal.transcript)
        assert b"AI>\r\nQuery analysis" in output
        assert b"\x1b[1;36mQuery analysis" not in output
        terminal.finish()


def exercise_streaming_answers(executable, env, port):
    write_config(env, '[agent]\nbackend="env"\ntimeout_secs=5')
    live = dict(env, TERM="xterm")
    MockService.release_stream.clear()

    def screen():
        return visible_terminal(b"".join(terminal.transcript))

    def ask(question):
        os.write(terminal.fd, question.encode() + b"\r")

    try:
        with Terminal(executable, live, port, mode="agent", size=(24, 80)) as terminal:
            terminal.expect("agent> ")
            # Partial text is drawn before the turn completes and replaces the spinner.
            ask("stream-gated")
            terminal.expect("Streaming first part.")
            assert MockService.model_requests[-1]["stream"] is True
            assert "AI thinking" not in screen()
            assert b"Streaming second part." not in b"".join(terminal.transcript)
            # Type-ahead is not echoed into the live region; readline gets it later.
            os.write(terminal.fd, b"zz")
            terminal.pump(0.3)
            assert "zz" not in screen(), screen()
            MockService.release_stream.set()
            terminal.expect("agent> ")
            MockService.release_stream.clear()
            terminal.expect("zz")
            os.write(terminal.fd, b"\x15")  # Ctrl+U discards the buffered type-ahead.
            final = screen()
            assert "AI> Streaming first part. Streaming second part." in final.splitlines(), final
            assert final.count("Streaming first part.") == 1, final
            assert "Streaming first part. Streaming second part." in final
            assert "AI thinking" not in final
            # Only the authoritative final answer enters the conversation.
            terminal.send("stream-follow-up", "agent> ")
            history = MockService.model_requests[-1]["messages"]
            assert [m["content"] for m in history if m["role"] == "assistant"] == [
                "Streaming first part. Streaming second part."]

            # A broken stream and Ctrl+C both erase the partial answer entirely.
            ask("stream-broken")
            terminal.expect("Partial streamed text")
            MockService.release_stream.set()
            terminal.expect("AI error: ")
            terminal.expect("agent> ")
            MockService.release_stream.clear()
            assert "Partial streamed text" not in screen(), screen()
            ask("stream-wait")
            terminal.expect("Waiting stream text")
            os.write(terminal.fd, b"\x03")
            terminal.expect("AI request interrupted.")
            terminal.expect("agent> ")
            MockService.release_stream.set()
            MockService.release_stream.clear()
            assert "Waiting stream text" not in screen(), screen()
            terminal.send("stream-after-errors", "agent> ")
            assert len(MockService.model_requests[-1]["messages"]) == 6, "Failed streams must not enter dialogue"

            # Long answers show only a tail while streaming, then print in full.
            ask("stream-long")
            terminal.expect("earlier lines; the full answer is shown when complete")
            terminal.expect("agent> ")
            final = screen()
            for n in range(40):
                assert final.count(f"row {n}\n") == 1 or final.endswith(f"row {n}"), (n, final)
            assert "earlier lines" not in final

            # A resize mid-stream erases the live region and falls back to the spinner.
            ask("stream-resize")
            terminal.expect("Resize stream text")
            before = len(b"".join(terminal.transcript))
            terminal.resize(24, 60)
            terminal.expect("AI thinking")
            MockService.release_stream.set()
            terminal.expect("agent> ")
            MockService.release_stream.clear()
            assert b"AI thinking" in b"".join(terminal.transcript)[before:]
            final = screen()
            assert final.count("Resize stream text") == 1, final
            assert "completed after resize." in final and "AI thinking" not in final

            # Markdown frames use the same renderer as the final answer.
            ask("provider-markdown")
            terminal.expect("agent> ")
            final = screen()
            assert final.count("Query analysis") == 1 and "## Query analysis" not in final
            assert "SELECT 'a|b'," in final and "private-html-payload" not in final

            terminal.send("!set ai_stream false", "agent> ")
            terminal.send("stream-disabled", "agent> ")
            assert MockService.model_requests[-1]["stream"] is False
            assert "Mock answer grounded in cached records." in screen()
            terminal.send("!set ai_stream maybe", "agent> ")
            assert b"error" in b"".join(terminal.transcript).lower()
            terminal.finish()
        # Config can disable streaming; non-terminal sizes never stream.
        write_config(env, '[settings]\nai_stream=false\n[agent]\nbackend="env"')
        with Terminal(executable, live, port, mode="agent", size=(24, 80)) as terminal:
            terminal.expect("agent> ")
            terminal.send("config-disabled", "agent> ")
            assert MockService.model_requests[-1]["stream"] is False
            terminal.finish()
        write_config(env, '[agent]\nbackend="env"')
        with Terminal(executable, live, port, mode="agent", size=(4, 80)) as terminal:
            terminal.expect("agent> ")
            terminal.send("tiny-terminal", "agent> ")
            assert MockService.model_requests[-1]["stream"] is False
            terminal.finish()
    finally:
        MockService.release_stream.set()


def exercise_sql_compatibility_and_sharing_consent(executable, env, port):
    with tempfile.TemporaryDirectory(prefix="bendsql-privacy-home-") as home:
        local = dict(env, HOME=home, TERM="xterm")
        write_config(local, '[agent]\nbackend="env"', consent=False, standard_prompt=False)
        prompt = f"root@127.0.0.1:{port}/default/default> "
        before = len(MockService.model_requests)
        def command(terminal, text):
            os.write(terminal.fd, text.encode() + b"\r")
            terminal.expect("\x1b[?2004l")
            terminal.expect("\x1b[?2004h")
            terminal.expect(prompt)
        with Terminal(executable, local, port, mode=None, startup_prompt=prompt) as terminal:
            terminal.expect(prompt)
            command(terminal, "SELECT 123456;")
            command(terminal, "CREATE USER local_user IDENTIFIED BY 'private-credential';")
            command(terminal, "!context")
            assert b"No cached queries" in b"".join(terminal.transcript)
            assert len(MockService.model_requests) == before
            terminal.finish()
        history = os.path.join(home, ".bendsql_history")
        with open(history) as file: persisted = file.read()
        assert "SELECT 123456" in persisted and "private-credential" in persisted
        logs = b""
        for directory, _, files in os.walk(home):
            for name in files:
                if name.startswith("bendsql.log"):
                    with open(os.path.join(directory, name), "rb") as file: logs += file.read()
        assert b"SELECT 123456" in logs, "SQL-only --log-level must remain effective"
        assert b"private-credential" in logs, "SQL-only logs match main; AI redaction must not affect them"
        with Terminal(executable, local, port, mode=None, startup_prompt=prompt) as terminal:
            terminal.expect(prompt)
            count = len(MockService.sql_requests)
            os.write(terminal.fd, b"\x1b[A\r")
            terminal.expect("\x1b[?2004l")
            terminal.expect("\x1b[?2004h")
            terminal.expect(prompt)
            wait_http_count(MockService.sql_requests, count + 1)
            assert MockService.sql_requests[-1] == "CREATE USER local_user IDENTIFIED BY 'private-credential'"
            for statement in ["SELECT token FROM t;", "SELECT * FROM t WHERE sig=1;", "SELECT secret FROM vault;", "CREATE STAGE typo;", "ALTER USER alice;"]:
                command(terminal, statement)
            # Bracketed paste exercises a statement larger than the removed 64 KiB cap.
            sql = "INSERT INTO bulk_input VALUES " + ",".join("('value')" for _ in range(9000)) + ";"
            payload = b"\x1b[200~" + sql.encode() + b"\x1b[201~\r"
            deadline = time.monotonic() + 20
            large_count = len(MockService.sql_requests)
            while payload or len(MockService.sql_requests) < large_count + 1:
                if time.monotonic() > deadline: raise AssertionError("Large SQL paste did not execute")
                readable, writable, _ = select.select([terminal.fd], [terminal.fd] if payload else [], [], 0.1)
                if readable:
                    chunk = os.read(terminal.fd, 65536)
                    terminal.pending += chunk
                    terminal.transcript.append(chunk)
                if writable:
                    written = os.write(terminal.fd, payload[:1024])
                    payload = payload[written:]
            terminal.expect("\x1b[?2004l")
            terminal.expect("\x1b[?2004h")
            terminal.expect(prompt)
            assert len(MockService.sql_requests[-1]) > 64 * 1024
            os.write(terminal.fd, b"!ask private-ai-question\r")
            terminal.expect("[y/N] ")
            assert len(MockService.model_requests) == before
            os.write(terminal.fd, b"\r")
            terminal.expect("AI data sharing not approved")
            terminal.expect(prompt)
            assert len(MockService.model_requests) == before
            os.write(terminal.fd, b"!mode smart\r")
            terminal.expect("[y/N] ")
            os.write(terminal.fd, b"yes\r")
            terminal.expect("Switched to smart mode.")
            terminal.expect(prompt)
            for statement in [
                "CREATE USER new_user IDENTIFIED BY 'private-credential'",
                "CREATE STAGE s URL='s3://bucket' CONNECTION=(access_key_id='id',secret_access_key='private-credential')",
                "CREATE CONNECTION c STORAGE_TYPE='s3' SECRET_ACCESS_KEY='private-credential'",
                "COPY INTO t FROM 's3://bucket' CONNECTION=(AWS_KEY_ID='id',AWS_SECRET_KEY='private-credential')",
                "SELECT SENSITIVE_PREVIEW", "SELECT SENSITIVE_ERROR",
            ]:
                command(terminal, f"!sql {statement};")
            command(terminal, "!context")
            output = b"".join(terminal.transcript)
            assert b"Query context cached" not in output
            assert b"may contain credentials or sensitive data" in output
            assert b"exceeded its memory budget" not in output
            command(terminal, "SELECT 1;")
            command(terminal, "!ask approved-analysis")
            assert len(MockService.model_requests) == before + 1
            records = last_context()["query_records"]
            assert len(records) == 1 and records[0]["sql"] == "SELECT 1"
            request = json.dumps(MockService.model_requests[-1])
            for private in ["private-credential", "private-result-secret", "private-error-secret", "SELECT 123456"]:
                assert private not in request
            assert "### README.md" not in request and "Directory:" not in request
            # A new backend selection requires fresh consent even though old query
            # evidence is retained. Config allow_data_sharing would be explicit opt-in.
            command(terminal, "!backend env")
            os.write(terminal.fd, b"!ask not-approved-after-switch\r")
            terminal.expect("[y/N] ")
            assert len(MockService.model_requests) == before + 1
            os.write(terminal.fd, b"n\r")
            terminal.expect("AI data sharing not approved")
            terminal.expect(prompt)
            terminal.finish()
        with open(history) as file: persisted = file.read()
        assert "approved-analysis" not in persisted and "private-ai-question" not in persisted
        assert "CREATE USER local_user" in persisted and "private-credential" in persisted
        for statement in ["SELECT token FROM t", "sig=1", "SELECT secret FROM vault", "CREATE STAGE typo", "ALTER USER alice"]:
            assert statement in persisted
        assert "new_user" not in persisted and "secret_access_key" not in persisted.lower()
        # Explicitly disabling SQL history is supported independently of AI use.
        os.unlink(history)
        write_config(local, '[settings]\nsave_history=false', consent=False)
        with Terminal(executable, local, port, mode=None) as terminal:
            terminal.expect("sql> ")
            terminal.send("SELECT 2;", "sql> ")
            terminal.finish()
        assert not os.path.exists(history)
        # Re-enabling save_history in a SQL-only session takes effect again.
        with Terminal(executable, local, port, mode=None) as terminal:
            terminal.expect("sql> ")
            terminal.send("!set save_history true", "sql> ")
            terminal.send("SELECT token FROM t WHERE sig=1;", "sql> ")
            terminal.finish()
        with open(history) as file: assert "SELECT token FROM t WHERE sig=1" in file.read()
        # AI activation preserves SQL navigation while keeping disk frozen.
        os.unlink(history)
        write_config(local, '[agent]\nbackend="env"', consent=False)
        with Terminal(executable, local, port, mode=None) as terminal:
            terminal.expect("sql> ")
            terminal.send("SELECT secret FROM vault;", "sql> ")
            os.write(terminal.fd, b"!ask hello\r")
            terminal.expect("[y/N] ")
            os.write(terminal.fd, b"y\r")
            terminal.expect("AI> ")
            terminal.expect("sql> ")
            count = len(MockService.sql_requests)
            os.write(terminal.fd, b"\x1b[A\r")
            terminal.expect("\x1b[?2004l")
            terminal.expect("\x1b[?2004h")
            terminal.expect("sql> ")
            wait_http_count(MockService.sql_requests, count + 1)
            assert MockService.sql_requests[-1] == "SELECT secret FROM vault"
            terminal.send("!set save_history false", "sql> ")
            terminal.send("!set save_history true", "sql> ")
            terminal.send("SELECT after_ai;", "sql> ")
            terminal.finish()
        with open(history) as file: saved = file.read()
        assert "after_ai" not in saved
        # Startup AI modes may read SQL history for navigation; it remains
        # separate from AI evidence and can never resume disk writes.
        write_config(local, '[agent]\nbackend="env"')
        for mode in ["smart", "agent"]:
            with Terminal(executable, local, port, mode=mode) as terminal:
                terminal.expect(f"{mode}> ")
                os.write(terminal.fd, b"\x1b[A")
                terminal.expect("SELECT secret FROM vault;")
                os.write(terminal.fd, b"\x03")
                terminal.expect(f"{mode}> ")
                terminal.send("!ask startup-history-check", f"{mode}> ")
                assert last_context()["query_records"] == []
                assert "vault" not in json.dumps(MockService.model_requests[-1])
                terminal.finish()
            with open(history) as file: assert file.read() == saved
        log_bytes = b""
        for directory, _, files in os.walk(home):
            for name in files:
                if name.startswith("bendsql.log"):
                    with open(os.path.join(directory, name), "rb") as file: log_bytes += file.read()
        assert b"SELECT after_ai" not in log_bytes, "Driver SQL payload must also be masked after AI activation"
        # Debug builds must handle a literal larger than the old large-stack
        # workaround could parse. Batch SQL does not invoke a real database.
        literal = "SELECT '" + "x" * (3 * 1024 * 1024) + "';\nSELECT 7;\n"
        dsn = f"databend://root:@127.0.0.1:{port}/default?sslmode=disable&login=disable"
        result = subprocess.run([executable, "--dsn", dsn, "--log-level", "off"], input=literal.encode(), env=local, capture_output=True, timeout=30)
        assert result.returncode == 0, result.stderr[-1000:]
        assert len(MockService.sql_requests[-2]) > 3 * 1024 * 1024 and MockService.sql_requests[-1] == "SELECT 7"
        for prefix, size in [("x", 400), ("X", 400), ("X", 3 * 1024 * 1024)]:
            insert = f"INSERT INTO hex_dump VALUES ({prefix}'" + "ab" * (size // 2) + "')"
            before = len(MockService.sql_requests)
            result = subprocess.run([executable, "--dsn", dsn, "--log-level", "off"], input=f"{insert}; SELECT 2;\n".encode(), env=local, capture_output=True, timeout=30)
            assert result.returncode == 0, result.stderr[-1000:]
            assert MockService.sql_requests[before:] == [insert, "SELECT 2"], "Neither the dump INSERT nor its successor may be lost"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--provider-only", action="store_true", help="Test the local OpenAI-compatible HTTP provider only")
    group.add_argument("--privacy-only", action="store_true", help="Test restored SQL history/logging and AI sharing boundaries")
    args = parser.parse_args()
    executable = os.path.abspath(args.executable)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockService)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    try:
        with tempfile.TemporaryDirectory(prefix="bendsql-agent-smoke-") as home:
            env = {
                key: value for key, value in os.environ.items()
                if not key.startswith("BENDSQL_")
            }
            env.update(
                HOME=home, TERM="dumb",
                BENDSQL_AGENT_BASE_URL=f"http://127.0.0.1:{port}/v1",
                BENDSQL_AGENT_MODEL="mock",
            )
            result = subprocess.run(
                [executable, "--mode", "smart"], env=env, capture_output=True, timeout=15
            )
            assert result.returncode != 0
            assert b"require an interactive terminal" in result.stderr
            assert not MockService.sql_requests
            if args.privacy_only:
                exercise_sql_compatibility_and_sharing_consent(executable, env, port)
            elif not args.provider_only:
                exercise_modes(executable, env, port)
                exercise_missing_model(executable, env, port)
                exercise_configured_backends(executable, env, port)
                exercise_cli_backends(executable, env, port)
                exercise_builtin_clis(executable, env, port)
                exercise_acp_backend(executable, env, port)
                exercise_smart_input_boundaries(executable, env, port)
                exercise_sql_cancellation(executable, env, port)
                exercise_default_backend_discovery(executable, env, port)
                exercise_transient_ai_spinner(executable, env, port)
                exercise_default_sql_modes_and_embedded_skills(executable, env, port)
                exercise_prompt_templates(executable, env, port)
                exercise_sql_compatibility_and_sharing_consent(executable, env, port)
            if not args.privacy_only:
                exercise_openai_provider(executable, env, port)
                exercise_ai_markdown_output(executable, env, port)
                exercise_streaming_answers(executable, env, port)
            # SQL-only history/logging are intentionally restored. Neither may
            # contain model requests, raw provider diagnostics or credentials.
            for root, _, files in os.walk(home):
                for name in files:
                    if name.startswith("bendsql.log") or name == ".bendsql_history":
                        with open(os.path.join(root, name), "rb") as file: data = file.read()
                        for forbidden in [b"private-provider-key", b"private-provider-diagnostic", b"Mock answer grounded in cached records."]:
                            assert forbidden not in data
        if args.privacy_only:
            print("PASS: SQL history/load, logging, large SQL, consent/no egress, sensitive context exclusion and backend consent reset")
            return
        if args.provider_only:
            print("PASS: mock OpenAI provider; request/auth/config, Databend skill, multi-turn evidence, errors/budgets, no SQL execution, clear, terminal Markdown/plain presentation and live streaming")
            return
        print(
            "PASS: mock OpenAI provider; three-mode PTY sessions, SQL/AI separation, partial/failed "
            "context, multiline guard, clear, query IDs, missing model, TTY and disk privacy; "
            "configured backends, headers, priority, switching and fail-closed configuration; "
            "Codex/Claude mock CLIs, stdin context, env isolation, errors, budgets and process cleanup; "
            "zero-config built-ins, aliases, partial overrides and disable controls; "
            "Pi protocol, safety settings, old-version rejection and process cleanup; "
            "ACP SDK lifecycle, capability denial, bounded transport, cancel handshake and cleanup; "
            "whole-input smart routing, live parser settings and SQL cancellation boundaries; "
            "quiet startup, default CLI priority, explicit selection and no runtime fallback; "
            "animated AI spinner, clean final screen on success/error/cancel/timeout, no request debug output; "
            "default SQL REPL, !mode Tab choices, lazy SQL-only completion and packaged read-only skills; "
            "complete configured prompt templates, {mode}, emoji, old variables and live prompt changes; "
            "AI Markdown/plain presentation, unchanged raw history, no suggested SQL execution or residual spinner; "
            "live streamed answers, tail-limited frames, erased partials on error/cancel/resize and ai_stream settings"
        )
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
