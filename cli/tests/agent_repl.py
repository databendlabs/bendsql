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

import errno
import http.server
import json
import os
import pty
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time


class MockService(http.server.BaseHTTPRequestHandler):
    sql_requests = []
    model_requests = []
    model_headers = []
    kill_requests = []
    page_requests = []
    release_queries = threading.Event()
    delayed_query_ids = set()

    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/v1/query/") and self.path.endswith("/kill"):
            self.kill_requests.append(self.path)
            query_id = self.path.split("/")[-2]
            if query_id in self.delayed_query_ids:
                self.release_queries.wait(10)
            response = {}
        elif self.path == "/v1/chat/completions":
            self.model_requests.append(request)
            self.model_headers.append({key.lower(): value for key, value in self.headers.items()})
            response = {
                "choices": [
                    {"message": {"content": "Mock answer grounded in cached records."}}
                ]
            }
        elif self.path == "/v1/query":
            sql = request["sql"]
            self.sql_requests.append(sql)
            fail = sql.startswith("Not SQL") or "SPOOF_INTERRUPTED" in sql
            query_id = f"query-{len(self.sql_requests)}"
            if "BLOCK_SUBMISSION" in sql:
                self.release_queries.wait(10)
            if "STALL_KILL" in sql:
                self.delayed_query_ids.add(query_id)
            data = (
                [["mock-version"]]
                if "version()" in sql.lower()
                else [[str(n)] for n in range(50)]
            )
            response = {
                "id": query_id,
                "node_id": None,
                "session_id": None,
                "session": None,
                "schema": [{"name": "value", "type": "String"}],
                "data": [] if fail or "BLOCK_PAGE" in sql else data,
                "state": "Failed" if fail else "Running" if "BLOCK_PAGE" in sql else "Succeeded",
                "settings": None,
                "error": (
                    {"code": 1005, "message": "Interrupted by Ctrl+C in SPOOF_INTERRUPTED result" if "SPOOF_INTERRUPTED" in sql else "Mock syntax error", "detail": None}
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
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client cancellation deliberately abandons pending responses.

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
    def __init__(self, executable, env, port, mode=None, backend=None):
        args = [
            executable, "--agent", "--no-auto-complete", "--dsn",
            f"databend://root:@127.0.0.1:{port}/default?sslmode=disable&login=disable",
            "--output", "null",
        ]
        if mode:
            args.extend(["--mode", mode])
        if backend:
            args.extend(["--backend", backend])
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execve(executable, args, env)
        self.pending = b""
        self.transcript = []
        self.finished = False

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

    def send(self, line, prompt="smart> "):
        os.write(self.fd, line.encode() + b"\r")
        self.expect(prompt)

    def finish(self):
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
    with Terminal(executable, env, port) as terminal:
        # Help includes the prompt strings too; wait until the actual REPL starts.
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("smart> ")
        assert len(MockService.sql_requests) == 1  # Startup version query.
        terminal.send("SELECT 1;")
        assert len(MockService.sql_requests) == 2
        assert not MockService.model_requests
        terminal.send("/context")
        assert b"cached 30/50 rows" in b"".join(terminal.transcript)

        terminal.send("/mode agent", "agent> ")
        terminal.send("SELECT 2;", "agent> ")
        assert len(MockService.sql_requests) == 2
        assert len(MockService.model_requests) == 1
        record = last_context()["query_records"][0]
        assert record["id"] == "Q1" and record["status"] == "success"
        assert len(record["preview"]["rows"]) == 30
        assert record["preview"]["complete"] is False
        assert record["stats"]["read_rows"] == 1000

        terminal.send("/mode sql", "sql> ")
        terminal.send("Not SQL;", "sql> ")
        assert len(MockService.sql_requests) == 3
        assert len(MockService.model_requests) == 1
        terminal.send("/mode smart")
        terminal.send("/ask Explain Q2")
        failed = next(r for r in last_context()["query_records"] if r["id"] == "Q2")
        assert failed["status"] == "failed"
        assert "Mock syntax error" in failed["error"]

        terminal.send("SELECT * FROM", "smart(sql)> ")
        terminal.send("/mode agent", "smart(sql)> ")
        assert len(MockService.sql_requests) == 3
        assert len(MockService.model_requests) == 2
        terminal.send("numbers;")
        assert len(MockService.sql_requests) == 4
        terminal.send("/clear")
        terminal.send("/context")
        terminal.send("/ask Hello after clearing")
        assert last_context()["query_records"] == []
        assert len(MockService.model_requests[-1]["messages"]) == 2
        terminal.send("SELECT 3;")
        terminal.send("/context")
        assert b"Q4: success" in b"".join(terminal.transcript)
        terminal.finish()


def exercise_missing_model(executable, env, port):
    env = {key: value for key, value in env.items() if not key.startswith("BENDSQL_AGENT_")}
    sql_count = len(MockService.sql_requests)
    model_count = len(MockService.model_requests)
    with Terminal(executable, env, port, mode="sql") as terminal:
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("sql> ")
        terminal.send("/ask Hello", "sql> ")
        assert b"Set BENDSQL_AGENT_BASE_URL" in b"".join(terminal.transcript)
        terminal.send("SELECT 1;", "sql> ")
        assert len(MockService.sql_requests) == sql_count + 2
        assert len(MockService.model_requests) == model_count
        terminal.finish()


def write_config(env, text):
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
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("smart> ")
        terminal.send("/backend")
        assert b"Current backend: second" in b"".join(terminal.transcript)
        terminal.send("/ask Initial question")
        assert MockService.model_requests[-1]["model"] == "second-model"
        assert MockService.model_requests[-1]["max_tokens"] == 1024
        assert MockService.model_headers[-1]["x-project"] == "example"
        assert "authorization" not in MockService.model_headers[-1]
        terminal.send("SELECT 1;")
        terminal.send("/ask Follow-up question")
        assert len(MockService.model_requests[-1]["messages"]) == 4
        terminal.send("/backend first")
        terminal.send("/ask Explain Q1")
        assert MockService.model_requests[-1]["model"] == "first-model"
        assert MockService.model_headers[-1]["authorization"] == "Bearer private-token"
        assert MockService.model_headers[-1]["x-custom-key"] == "private-header"
        assert len(MockService.model_requests[-1]["messages"]) == 2
        assert last_context()["query_records"][0]["id"] == "Q1"
        count = len(MockService.model_requests)
        terminal.send("/backend broken")
        assert len(MockService.model_requests) == count
        terminal.send("/ask Still on first?")
        assert MockService.model_requests[-1]["model"] == "first-model"
        assert len(MockService.model_requests[-1]["messages"]) == 4
        terminal.send("/backend env")
        terminal.send("/ask Explicit environment backend")
        assert MockService.model_requests[-1]["model"] == "mock"
        assert len(MockService.model_requests[-1]["messages"]) == 2
        assert last_context()["query_records"][0]["id"] == "Q1"
        terminal.send("/clear")
        terminal.send("/ask After clearing")
        assert last_context()["query_records"] == []
        assert len(MockService.model_requests[-1]["messages"]) == 2
        terminal.finish()
        output = b"".join(terminal.transcript)
        for secret in [b"private-token", b"private-header", b"private-url-secret"]:
            assert secret not in output

    # Without a CLI override, the selected config wins over legacy environment.
    with Terminal(executable, env, port) as terminal:
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("smart> ")
        terminal.send("/ask Config default")
        assert MockService.model_requests[-1]["model"] == "first-model"
        terminal.finish()

    # Declaring backends without selecting one must not implicitly use env.
    write_config(env, config.replace('backend = "first"\n', "", 1))
    with Terminal(executable, env, port) as terminal:
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("smart> ")
        count = len(MockService.model_requests)
        terminal.send("/ask No default selected")
        assert len(MockService.model_requests) == count
        assert b"Select an AI backend" in b"".join(terminal.transcript)
        terminal.send("/backend second")
        terminal.send("/ask Explicit selection")
        assert MockService.model_requests[-1]["model"] == "second-model"
        terminal.finish()

    # Semantic and syntax errors disable AI, but the explicit SQL DSN still works.
    for invalid in [
        "[agent]\nbackend=123\nsecret='private-config-secret'",
        "[agent\nsecret='private-config-secret'",
    ]:
        write_config(env, invalid)
        with Terminal(executable, env, port) as terminal:
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
            terminal.expect("smart> ")
            count = len(MockService.model_requests)
            sql_count = len(MockService.sql_requests)
            terminal.send("/ask Bad configuration")
            terminal.send("/backend env")
            assert len(MockService.model_requests) == count
            terminal.send("SELECT 1;")
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
    while any(process_running(pid) for pid in pids):
        if time.monotonic() >= deadline:
            for pid in pids:
                if process_running(pid):
                    os.kill(pid, signal.SIGKILL)
            raise AssertionError("CLI cancellation left a running child or descendant")
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
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
            terminal.expect("smart> ")
            terminal.send("SELECT 1;")
            sql_count = len(MockService.sql_requests)
            for backend in ["claude", "codex"]:
                if backend == "codex":
                    terminal.send("/backend codex")
                count = len(cli_records(record_path))
                terminal.send("/ask Please explain Q1; do-not-put-this-in-argv")
                record = cli_records(record_path)[-1]
                assert len(cli_records(record_path)) == count + 1
                args = record["argv"]
                assert "do-not-put-this-in-argv" not in " ".join(args)
                assert "SELECT 1" not in " ".join(args)
                assert not any("dangerously" in arg for arg in args)
                expected_model = f"configured-{backend}-model"
                assert args[args.index("--model") + 1] == expected_model
                if backend == "claude":
                    assert args[args.index("--tools") + 1] == ""
                    assert "--safe-mode" in args and "--no-session-persistence" in args
                    assert record["env"]["ANTHROPIC_API_KEY"] == "private-claude-key"
                    assert "OPENAI_API_KEY" not in record["env"]
                else:
                    assert args[args.index("--sandbox") + 1] == "read-only"
                    assert "--ignore-user-config" in args and "--ephemeral" in args
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
                for question in ["exit-error", "bad-json", "failed-turn", "overflow-stdout", "overflow-stderr"]:
                    terminal.send(f"/ask {question}")
                    assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("/ask After failed requests")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("/ask long-answer")
                assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
                terminal.send("/clear")
                terminal.send("SELECT 1;")
                sql_count += 1
                # Cancel while the process and a descendant are holding both pipes.
                count = len(cli_records(record_path))
                os.write(terminal.fd, b"/ask block-cli\r")
                record = wait_cli_record(record_path, count + 1)
                os.write(terminal.fd, b"\x03")
                terminal.expect("AI request interrupted.")
                terminal.expect("smart> ")
                wait_stopped(record)
                terminal.send("/ask After cancellation")
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                # Also exercise deadline-based cleanup, not only user cancellation.
                terminal.send("/ask block-cli")
                wait_stopped(cli_records(record_path)[-1])
                terminal.send("/ask After timeout")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                assert len(MockService.sql_requests) == sql_count
                assert len(MockService.model_requests) == api_count
            count = len(cli_records(record_path))
            terminal.send("/backend no-consent")
            terminal.send("/backend not-installed")
            assert len(cli_records(record_path)) == count
            terminal.send("/ask Still on codex")
            assert "--json" in cli_records(record_path)[-1]["argv"]
            terminal.finish()
            output = b"".join(terminal.transcript)
            assert b"allow_external_agent = true" in output
            assert b"exceeded its byte budget" in output
            assert b"timed out" in output
            for secret in [b"private-cli-secret", b"private-db-password", b"private-db-dsn", b"private-unrelated-secret", b"private-codex-key", b"private-claude-key"]:
                assert secret not in output
            assert b"\x1b[31mMock CLI" not in output


def exercise_builtin_clis(executable, env, port):
    # No backend tables, model variables or consent flags are required.
    config_path = os.path.join(env["HOME"], ".config", "bendsql", "config.toml")
    if os.path.exists(config_path):
        os.unlink(config_path)
    source = os.path.join(os.path.dirname(__file__), "mock_agent_cli.py")
    with open(source) as file:
        contents = file.read()
    record_path = os.path.join(env["HOME"], "builtin_cli_requests.jsonl")
    api_count = len(MockService.model_requests)
    with tempfile.TemporaryDirectory(prefix="bendsql-builtin-bin-") as directory:
        for name in ["codex", "claude", "pi", "amp"]:
            path = os.path.join(directory, name)
            with open(path, "w") as file:
                file.write(f"#!{sys.executable}\n{contents}")
            os.chmod(path, 0o700)
        cli_env = dict(env, PATH=directory + os.pathsep + env.get("PATH", os.defpath),
                       AMP_API_KEY="private-amp-key", GEMINI_API_KEY="private-pi-key")
        for name in list(cli_env):
            if name.startswith("BENDSQL_AGENT_"):
                del cli_env[name]
        for name in ["local-claude", "local-codex", "local-pi", "local-amp"]:
            with Terminal(executable, cli_env, port, backend=name) as terminal:
                terminal.expect("Use /clear before discussing unrelated or sensitive data.")
                terminal.expect("smart> ")
                terminal.send("/backend")
                output = b"".join(terminal.transcript)
                assert f"Current backend: {name}".encode() in output
                for builtin in [b"local-claude", b"local-codex", b"local-pi", b"local-amp"]:
                    assert builtin in output
                terminal.send("SELECT 1;")
                sql_count = len(MockService.sql_requests)
                terminal.send("/ask Explain Q1 without backend configuration")
                record = cli_records(record_path)[-1]
                assert "--model" not in record["argv"]
                assert "CLI_TEST_RECORD" not in record["env"]
                assert len(record["messages"]) == 2
                assert json.loads(record["messages"][-1]["content"])["query_records"][0]["id"] == "Q1"
                if name == "local-codex":
                    assert "--json" in record["argv"]
                    terminal.send("/backend claude-code")
                    terminal.send("/ask Switch to built-in Claude")
                    assert "--print" in cli_records(record_path)[-1]["argv"]
                elif name == "local-claude":
                    assert "--print" in record["argv"]
                    terminal.send("/backend codex")
                    terminal.send("/ask Switch to built-in Codex")
                    assert "--json" in cli_records(record_path)[-1]["argv"]
                elif name == "local-pi":
                    assert "--no-tools" in record["argv"] and "--no-session" in record["argv"]
                    assert "--no-extensions" in record["argv"] and "--no-mcp" in record["argv"]
                    assert record["env"]["PI_OFFLINE"] == "1"
                    assert record["env"]["GEMINI_API_KEY"] == "private-pi-key"
                    assert "AMP_API_KEY" not in record["env"]
                    terminal.send("/backend amp")
                    terminal.send("/ask Switch to built-in Amp")
                    assert "--stream-json" in cli_records(record_path)[-1]["argv"]
                else:
                    assert "--stream-json" in record["argv"] and "--settings-file" in record["argv"]
                    assert record["policy"]["amp.tools.disable"] == ["*"]
                    assert record["policy"]["amp.mcpPermissions"][0]["action"] == "reject"
                    assert record["directory_files"] == ["amp-policy.json"]
                    assert record["env"]["AMP_API_KEY"] == "private-amp-key"
                    assert "GEMINI_API_KEY" not in record["env"]
                    terminal.send("/backend pi")
                    terminal.send("/ask Switch to built-in Pi")
                    assert "--no-tools" in cli_records(record_path)[-1]["argv"]
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                assert len(MockService.sql_requests) == sql_count
                assert len(MockService.model_requests) == api_count
                terminal.finish()
        # Exercise failures/cancellation for each new adapter using its native wire format.
        write_config(cli_env, '[agent]\ntimeout_secs=2')
        for name in ["pi", "amp"]:
            with Terminal(executable, cli_env, port, backend=name) as terminal:
                terminal.expect("Use /clear before discussing unrelated or sensitive data.")
                terminal.expect("smart> ")
                terminal.send("SELECT 1;")
                sql_count = len(MockService.sql_requests)
                help_count = len(cli_records(record_path + ".help"))
                terminal.send("/ask Explain Q1")
                assert len(cli_records(record_path + ".help")) == help_count + 1
                for question in ["exit-error", "bad-json", "failed-turn", "overflow-stdout", "overflow-stderr", "tool-policy"]:
                    terminal.send(f"/ask {question}")
                    assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("/ask After failures")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                terminal.send("/ask long-answer")
                assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
                assert len(cli_records(record_path + ".help")) == help_count + 1
                terminal.send("/clear")
                count = len(cli_records(record_path))
                os.write(terminal.fd, b"/ask block-cli\r")
                record = wait_cli_record(record_path, count + 1)
                os.write(terminal.fd, b"\x03")
                terminal.expect("AI request interrupted.")
                terminal.expect("smart> ")
                wait_stopped(record)
                terminal.send("/ask After cancellation")
                assert len(cli_records(record_path)[-1]["messages"]) == 2
                terminal.send("/ask block-cli")
                wait_stopped(cli_records(record_path)[-1])
                terminal.send("/ask After timeout")
                assert len(cli_records(record_path)[-1]["messages"]) == 4
                assert len(MockService.sql_requests) == sql_count
                assert len(cli_records(record_path + ".help")) == help_count + 2
                terminal.finish()
                for secret in [b"private-cli-secret", b"private-thinking", b"private-amp-key", b"private-pi-key"]:
                    assert secret not in b"".join(terminal.transcript)
        # Old versions fail before receiving context. No downgrade and no service fallback.
        for name in ["pi", "amp"]:
            write_config(cli_env, f'[agent.backends.local-{name}]\nenv_allowlist=["CLI_TEST_OLD"]')
            old_env = dict(cli_env, CLI_TEST_OLD="1")
            with Terminal(executable, old_env, port, backend=name) as terminal:
                terminal.expect("Use /clear before discussing unrelated or sensitive data.")
                terminal.expect("smart> ")
                count = len(cli_records(record_path))
                terminal.send("/ask Context must not be delivered")
                assert len(cli_records(record_path)) == count
                assert b"Context was not sent" in b"".join(terminal.transcript)
                assert cli_records(record_path + ".help")[-1]["stdin"] == ""
                terminal.send("SELECT 1;")
                terminal.finish()
        assert all(record["stdin"] == "" for record in cli_records(record_path + ".help"))
        assert len(MockService.model_requests) == api_count
        # Override just a model on a built-in profile; type/adapter/consent inherit.
        write_config(cli_env, '[agent]\nbackend="local-codex"\n[agent.backends.local-codex]\nmodel="custom-builtin-model"')
        with Terminal(executable, cli_env, port, backend="codex") as terminal:
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
            terminal.expect("smart> ")
            terminal.send("/ask Built-in model override")
            args = cli_records(record_path)[-1]["argv"]
            assert args[args.index("--model") + 1] == "custom-builtin-model"
            terminal.finish()
        # A disabled canonical profile cannot be bypassed through its alias.
        write_config(cli_env, '[agent.backends.local-codex]\nallow_external_agent=false')
        with Terminal(executable, cli_env, port, backend="codex") as terminal:
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
            terminal.expect("smart> ")
            count = len(cli_records(record_path))
            terminal.send("/ask Disabled built-in")
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
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
            terminal.expect("smart> ")
            assert not cli_records(path), "Adapter must not start until a question"
            terminal.send("SELECT 1;")
            sql_count = len(MockService.sql_requests)
            terminal.send("/ask Explain Q1; do-not-put-this-in-argv")
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
            terminal.send("/ask Follow-up")
            prompt = acp_records(path, "prompt")[-1]
            assert len(prompt["messages"]) == 4
            wait_stopped(prompt)
            failures = ["rpc-error", "bad-json", "wrong-session", "malformed-update", "unsupported-content",
                        "overflow-frame", "overflow-wire", "overflow-stderr", "tool-call", "permission",
                        "read-file", "write-file", "terminal", "extension", "failed-turn", "no-answer"]
            for question in failures:
                terminal.send(f"/ask {question}")
                prompt = acp_records(path, "prompt")[-1]
                assert len(prompt["messages"]) == 6
                wait_stopped(prompt)
            replies = acp_records(path, "client_reply")
            assert any(reply["reply"].get("result", {}).get("outcome", {}).get("outcome") == "cancelled" for reply in replies)
            assert sum(reply["reply"].get("error", {}).get("code") == -32601 for reply in replies) >= 4
            terminal.send("/ask After errors")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 6
            terminal.send("/ask long-answer")
            assert b"[Answer truncated at 8 KiB]" in b"".join(terminal.transcript)
            terminal.send("/ask max-tokens")
            assert b"[ACP answer truncated by model output limit]" in b"".join(terminal.transcript)
            terminal.send("/clear")
            count = len(acp_records(path, "prompt"))
            os.write(terminal.fd, b"/ask block-acp\r")
            prompt = wait_acp_prompt(path, count + 1)
            os.write(terminal.fd, b"\x03")
            terminal.expect("AI request interrupted.")
            terminal.expect("smart> ")
            wait_stopped(prompt)
            assert acp_records(path, "cancel")[-1]["pid"] == prompt["pid"]
            terminal.send("/ask After cancellation")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 2
            terminal.send("/ask block-acp")
            prompt = acp_records(path, "prompt")[-1]
            wait_stopped(prompt)
            assert acp_records(path, "cancel")[-1]["pid"] == prompt["pid"]
            terminal.send("/ask After timeout")
            assert len(acp_records(path, "prompt")[-1]["messages"]) == 4
            terminal.send("/ask ignore-cancel")
            wait_stopped(acp_records(path, "prompt")[-1])
            # Initialization failure/timeout must never receive a prompt/context.
            for backend in ["bad-version", "hang-init"]:
                count = len(acp_records(path, "prompt"))
                terminal.send(f"/backend {backend}")
                terminal.send("/ask Context must not be delivered")
                assert len(acp_records(path, "prompt")) == count
                init = acp_records(path, "initialize")[-1]
                wait_stopped(dict(init, child=None))
            terminal.send("/backend no-consent")
            assert len(MockService.model_requests) == api_count
            assert len(MockService.sql_requests) == sql_count
            terminal.send("SELECT 1;")
            terminal.finish()
            output = b"".join(terminal.transcript)
            for secret in [b"private-acp-secret", b"private-thinking", b"private-acp-auth", b"private-db-password", b"private-db-dsn", b"not-inherited"]:
                assert secret not in output
            assert b"\x1b[31mMock ACP" not in output


def exercise_smart_input_boundaries(executable, env, port):
    write_config(env, "")
    with Terminal(executable, env, port) as terminal:
        terminal.expect("Use /clear before discussing unrelated or sensitive data.")
        terminal.expect("smart> ")
        sql_count = len(MockService.sql_requests)
        api_count = len(MockService.model_requests)
        for text in ["SELECT 1; Explain the result", "DROP TABLE sensitive; Why would this happen?",
                     "/* example */ SELECT 1; explain", "SELECT 1; SELECT * FROM",
                     "SELCT 1;", "SELECT * FROM;", "GET file://result @stage"]:
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
        terminal.send("/sql SELECT 5|")
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
        terminal.send("/mode sql", "sql> ")
        terminal.send("Not SQL;", "sql> ")
        assert len(MockService.sql_requests) == sql_count + 9
        terminal.send("/mode agent", "agent> ")
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
    write_config(env, "")
    MockService.release_queries.clear()
    try:
        with Terminal(executable, env, port) as terminal:
            terminal.expect("Use /clear before discussing unrelated or sensitive data.")
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
            terminal.send("/ask Explain Q2")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q2")
            assert record["status"] == "interrupted" and record["query_id"] is None
            assert record["cancel_request_sent"] is False
            assert "state and Query ID may be unknown" in record["error"]
            MockService.release_queries.set()
            terminal.send("SELECT 2;")
            terminal.send("/sql SELECT SPOOF_INTERRUPTED;")
            assert len(MockService.kill_requests) == kills
            terminal.send("/ask Explain Q4")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q4")
            assert record["status"] == "failed" and record["cancel_request_sent"] is False
            MockService.release_queries.clear()
            pages = len(MockService.page_requests)
            os.write(terminal.fd, b"SELECT BLOCK_PAGE;\r")
            wait_http_count(MockService.page_requests, pages + 1)
            os.write(terminal.fd, b"\x03")
            terminal.expect("smart> ")
            assert len(MockService.kill_requests) == kills + 1
            terminal.send("/ask Explain Q5")
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
            terminal.send("/ask Explain Q6")
            record = next(r for r in last_context()["query_records"] if r["id"] == "Q6")
            assert record["status"] == "interrupted" and record["cancel_request_sent"] is False
            assert b"Cancellation request timed out" in b"".join(terminal.transcript)
            MockService.release_queries.set()
            terminal.finish()
    finally:
        MockService.release_queries.set()


def main():
    executable = os.path.abspath(sys.argv[1])
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
                [executable, "--agent"], env=env, capture_output=True, timeout=15
            )
            assert result.returncode != 0
            assert b"requires an interactive terminal" in result.stderr
            assert not MockService.sql_requests
            exercise_modes(executable, env, port)
            exercise_missing_model(executable, env, port)
            exercise_configured_backends(executable, env, port)
            exercise_cli_backends(executable, env, port)
            exercise_builtin_clis(executable, env, port)
            exercise_acp_backend(executable, env, port)
            exercise_smart_input_boundaries(executable, env, port)
            exercise_sql_cancellation(executable, env, port)
            assert not os.path.exists(os.path.join(home, ".bendsql_history"))
            for root, _, files in os.walk(home):
                for name in files:
                    if name.startswith("bendsql.log"):
                        assert os.path.getsize(os.path.join(root, name)) == 0
        print(
            "PASS: three-mode PTY sessions, SQL/AI separation, partial/failed "
            "context, multiline guard, clear, query IDs, missing model, TTY and disk privacy; "
            "configured backends, headers, priority, switching and fail-closed configuration; "
            "Codex/Claude mock CLIs, stdin context, env isolation, errors, budgets and process cleanup; "
            "zero-config built-ins, aliases, partial overrides and disable controls; "
            "Pi/Amp protocols, safety settings, old-version rejection and process cleanup; "
            "ACP SDK lifecycle, capability denial, bounded transport, cancel handshake and cleanup; "
            "whole-input smart routing, live parser settings and SQL cancellation boundaries"
        )
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
