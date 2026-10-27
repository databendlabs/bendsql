#!/usr/bin/env python3
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

"""Real CLI + HTTPS mock; stdlib/OpenSSL only, no Cloud credentials or Docker.

A loopback-only CONNECT proxy terminates TLS for the two official API hostnames
using a temporary test CA. It never forwards traffic or resolves those domains.
This exercises the production origin allowlist without a custom-origin escape.

Run: python3 cli/tests/cloud_webapi_https.py /path/to/bendsql
"""

import base64
import http.server
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import uuid
from urllib.parse import urlencode

TOKEN = "fake-cloud-https-secret"


def certificates(directory):
    directory.joinpath("extensions.cnf").write_text(
        "[server]\nbasicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:api.databend.com,DNS:api.databend.cn\n"
    )
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
         "-out", "ca.pem", "-days", "1", "-subj", "/CN=BendSQL Test CA"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key",
         "-out", "server.csr", "-subj", "/CN=localhost"],
        ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
         "-CAcreateserial", "-out", "server.pem", "-days", "1",
         "-extfile", "extensions.cnf", "-extensions", "server"],
    ]
    for command in commands:
        subprocess.run(["openssl", *command], cwd=directory, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)


def reply(data, next_uri=None):
    return {"id": "q1", "schema": [{"name": "n", "type": "Int64"}],
            "data": data, "state": "Succeeded", "session": {"database": "default"},
            "next_uri": next_uri, "stats": {
                "scan_progress": {"rows": 0, "bytes": 0},
                "write_progress": {"rows": 0, "bytes": 0},
                "result_progress": {"rows": len(data), "bytes": len(data)},
                "running_time_ms": 0}}


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def respond(self, status, body):
        body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Set-Cookie", "console-cookie=not-for-cloud; Path=/")
        self.send_header("Set-Cookie", "access_token=console-secret; Path=/")
        if self.server.state.emit_session_cookies:
            self.send_header("Set-Cookie", "session_id=mock-sql-session; Path=/; Secure; HttpOnly")
            self.send_header("Set-Cookie", "last_refresh_time=123; Path=/; Secure")
        self.end_headers()
        self.wfile.write(body)

    def do_CONNECT(self):
        # A local mock, not a general-purpose forwarding proxy. Unexpected
        # targets are rejected and no upstream network connection is made.
        if self.path not in {"api.databend.com:443", "api.databend.cn:443"}:
            self.send_error(403)
            return
        self.server.state.tunnels.append(self.path)
        self.send_response(200, "Connection established")
        self.end_headers()
        self.wfile.flush()
        try:
            self.request = self.server.context.wrap_socket(self.connection, server_side=True)
        except ssl.SSLError:
            return  # Expected when the CLI has not been given the test CA.
        self.setup()
        self.close_connection = True
        self.handle_one_request()

    def handle_request(self):
        state = self.server.state
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        state.records.append((self.command, self.path, dict(self.headers), body))
        if self.path.startswith("/v1/session/login"):
            self.respond(404, {"error": "not found"})
        elif self.command == "POST" and self.path == "/v1/query":
            next_uri = "/v1/query/q1/page/0" if state.cloud else None
            self.respond(200, reply([["1"]], next_uri))
        elif self.path == "/v1/query/q1/page/0":
            state.pages += 1
            if state.pages == 1:
                self.respond(503, {"error": "retry safe page"})
            else:
                self.respond(200, reply([["2"]], "/v1/query/q1/final"))
        elif self.path == "/v1/query/q1/final":
            self.respond(200, reply([]))
        else:
            self.respond(404, {"error": "unexpected route"})

    do_POST = do_GET = handle_request


def main():
    binary = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="bendsql-https-") as temporary:
        directory = Path(temporary)
        certificates(directory)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(directory / "server.pem", directory / "server.key")
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.state = server
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        proxy.context, proxy.state = context, server
        threads = [threading.Thread(target=service.serve_forever, daemon=True)
                   for service in (server, proxy)]
        for thread in threads:
            thread.start()
        ca = str(directory / "ca.pem")
        private = "databend://root:pw@127.0.0.1:" + str(server.server_port) + "/default?" + urlencode({
            "tls_ca_file": ca, "presign": "off", "retry_delay_secs": "0"})
        cloud = "databend://api.databend.com:443/default?" + urlencode({
            "tls_ca_file": ca, "cloud_api_token": TOKEN, "org_slug": "org-example",
            "retry_delay_secs": "0"})

        def run(name, args, environment=None, config=None, expected_cloud=False,
                authorization=None, expected_error=None, query="SELECT 1", emit_session_cookies=True):
            with tempfile.TemporaryDirectory(prefix="bendsql-home-") as home:
                if config:
                    target = Path(home) / ".bendsql"
                    target.mkdir()
                    target.joinpath("config.toml").write_text(config)
                env = {key: value for key, value in os.environ.items()
                       if not key.startswith("BENDSQL_") and "proxy" not in key.lower()}
                env.update(HOME=home, HTTPS_PROXY=f"http://127.0.0.1:{proxy.server_port}",
                           HTTP_PROXY="http://127.0.0.1:0", ALL_PROXY="http://127.0.0.1:0",
                           NO_PROXY="127.0.0.1,localhost")
                env.update(environment or {})
                server.records, server.pages, server.cloud, server.tunnels = [], 0, expected_cloud, []
                server.emit_session_cookies = emit_session_cookies
                result = subprocess.run([binary, "--non-interactive", f"--query={query}", *args],
                                        env=env, capture_output=True, text=True, timeout=15)
                text = result.stdout + result.stderr
                assert TOKEN not in text, name + ": token in output"
                if expected_error:
                    assert result.returncode != 0 and expected_error in text, (name, text)
                    assert not server.records, name
                else:
                    assert result.returncode == 0, (name, text)
                    assert "1" in result.stdout.splitlines(), (name, text)
                    assert "Cloud WebAPI does not guarantee transactions" not in result.stderr, name
                    for index, (method, path, headers, body) in enumerate(server.records):
                        headers = {key.lower(): value for key, value in headers.items()}
                        if expected_cloud:
                            assert headers.get("x-databendcloud-token") == TOKEN, name
                            assert headers.get("x-databendcloud-org") == "org-example", name
                            assert "authorization" not in headers, name
                            cookies = dict(part.strip().split("=", 1) for part in headers.get("cookie", "").split(";") if "=" in part)
                            assert cookies.get("cookie_enabled") == "true", name
                            assert set(cookies) == {"cookie_enabled", "session_id", "last_refresh_time"}, name
                            if index > 0 and emit_session_cookies:
                                assert cookies.get("session_id") == "mock-sql-session", name
                                assert cookies.get("last_refresh_time") == "123", name
                            else:
                                uuid.UUID(cookies["session_id"])
                                assert cookies["last_refresh_time"].isdigit(), name
                                if index == 0:
                                    initial_session_id = cookies["session_id"]
                                else:
                                    assert cookies["session_id"] == initial_session_id, name
                        else:
                            assert "x-databendcloud-token" not in headers, name
                            assert headers.get("authorization") == authorization, name
                        if path == "/v1/query":
                            assert "string_fields" not in json.loads(body), name
                    if expected_cloud:
                        assert server.tunnels, name + ": CLI did not use local CONNECT proxy"
                        assert all(target in {"api.databend.com:443", "api.databend.cn:443"}
                                   for target in server.tunnels), name
                        assert all(headers.get("host") in {"api.databend.com", "api.databend.com:443",
                                                           "api.databend.cn", "api.databend.cn:443"}
                                   for _, _, headers, _ in server.records), name
                        assert "2" in result.stdout.splitlines(), (name, text)
                        assert server.pages == 2, name + ": page 503 not retried"
                        assert any(method == "GET" and path.endswith("/final")
                                   for method, path, _, _ in server.records), name
                print("PASS:", name)

        try:
            run("official origin through local HTTPS proxy, query/page/final with retry", ["--dsn", cloud], expected_cloud=True)
            run("SQL session remains stable when proxy omits Set-Cookie", ["--dsn", cloud],
                expected_cloud=True, emit_session_cookies=False)
            run("China official origin through local HTTPS proxy", ["--dsn", cloud.replace("api.databend.com", "api.databend.cn")], expected_cloud=True)
            run("Cloud transaction statements are not incorrectly warned on", ["--dsn", cloud],
                expected_cloud=True, query="/* session */ BEGIN")
            tls_config = "[connection]\nhost='api.databend.com'\ntls=false\n"
            run("config TLS disable reports HTTPS requirement", ["--cloud-api-token=" + TOKEN, "--org=org-example"],
                config=tls_config, expected_error="requires HTTPS; sslmode=disable is not supported")
            no_ca = "databend://api.databend.com/default?" + urlencode({
                "cloud_api_token": TOKEN, "org_slug": "org-example", "retry_delay_secs": "0"})
            run("TLS still rejects the mock without its test CA", ["--dsn", no_ca],
                expected_error="error sending request")
            for endpoint in ["localhost:8443", "staging.example:8443", "api.databend.com:8443"]:
                untrusted = cloud.replace("api.databend.com:443", endpoint)
                run("production allowlist rejects " + endpoint, ["--dsn", untrusted],
                    expected_error="not a Gateway or private server")
            basic = "Basic " + base64.b64encode(b"root:pw").decode()
            run("global token does not replace explicit DSN credentials", ["--dsn", private],
                {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, authorization=basic)
            run("native transaction statements keep existing behavior", ["--dsn", private],
                authorization=basic, query="BEGIN")
            run("global token does not replace BENDSQL_DSN", [],
                {"BENDSQL_CLOUD_API_TOKEN": TOKEN, "BENDSQL_DSN": private}, authorization=basic)
            flags = ["--host=127.0.0.1", f"--port={server.server_port}", "--tls=true",
                     "--set", f"tls_ca_file={ca}", "--set", "presign=off"]
            cloud_flags = ["--host=api.databend.com", "--port=443", "--tls=true",
                           "--set", f"tls_ca_file={ca}", "--set", "retry_delay_secs=0"]
            run("environment token works for a credential-free CLI connection", [*cloud_flags, "--org=org-example"],
                {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, expected_cloud=True)
            run("global token does not replace user/password flags", [*flags, "--user=root", "--password=pw"],
                {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, authorization=basic)
            run("global token does not replace an Access Token", [*flags, "--set", "access_token=native-fixture"],
                {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, authorization="Bearer native-fixture")
            empty_basic = "Basic " + base64.b64encode(b"root:").decode()
            run("empty environment token means unset", flags, {"BENDSQL_CLOUD_API_TOKEN": ""}, authorization=empty_basic)
            config = f"[connection]\nhost='127.0.0.1'\nport={server.server_port}\ntls=true\nuser='root'\n[connection.args]\ntls_ca_file='{ca}'\npresign='off'\n"
            run("global token does not replace config-file user", [], {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, config, authorization=empty_basic)
            for config in ["[connection]\nport=8000\n", "[connection]\nhost='localhost'\n"]:
                run("explicit config default remains authoritative: " + config.splitlines()[1],
                    ["--cloud-api-token=", "--org=org-example"], config=config,
                    expected_error="not a Gateway or private server")
        finally:
            for service in (server, proxy):
                service.shutdown()
                service.server_close()
            for thread in threads:
                thread.join(timeout=3)


if __name__ == "__main__":
    main()
