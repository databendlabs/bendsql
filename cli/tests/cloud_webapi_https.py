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
        if (status == 200 and self.server.state.cloud
                and getattr(self.server.state, "keep_alive", False)
                and self.path.startswith("/v1/query")):
            body["session"].update(need_keep_alive=True, need_sticky=True)
            body["node_id"] = "sticky-node"
        body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Set-Cookie", "console-cookie=not-for-cloud; Path=/")
        self.send_header("Set-Cookie", "access_token=console-secret; Path=/")
        if self.server.state.emit_session_cookies:
            self.send_header("Set-Cookie", "session_id=mock-sql-session; Path=/; Secure; HttpOnly")
            self.send_header("Set-Cookie", "last_refresh_time=123; Path=/; Secure")
        if (self.server.state.cloud and getattr(self.server.state, "emit_session_header", True)
                and self.headers.get("X-DATABEND-CLIENT-CAPS") == "session_header"):
            self.send_header("X-DATABEND-SESSION", f"opaque-https-session-{self.server.state.session_version}")
            self.server.state.session_version += 1
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
        if self.path == "/v1/session/logout":
            self.respond(200, {"error": None})
        elif self.path.startswith("/v1/session/login"):
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
            "tls_ca_file": ca, "cloud_api_token": TOKEN, "org": "org-example",
            "retry_delay_secs": "0"})

        def run(name, args, environment=None, config=None, expected_cloud=False,
                authorization=None, expected_error=None, query="SELECT 1", emit_session_cookies=True,
                emit_session_header=True, keep_alive=False, expected_pagination=None):
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
                server.emit_session_header = emit_session_header
                server.keep_alive = keep_alive
                server.session_version = 0
                result = subprocess.run([binary, "--non-interactive", f"--query={query}", *args],
                                        env=env, capture_output=True, text=True, timeout=15)
                text = result.stdout + result.stderr
                assert TOKEN not in text, name + ": token in output"
                assert "opaque-https-session-" not in text, name + ": session in output"
                if expected_error:
                    assert result.returncode != 0 and expected_error in text, (name, text)
                    if expected_cloud and not emit_session_header:
                        assert [path for _, path, _, _ in server.records] == ["/v1/query"], name
                        assert "may already have executed" in text, name
                        assert not result.stdout.strip(), name + ": rows exposed without a valid session"
                    else:
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
                            assert "cookie" not in headers, name
                            assert headers.get("x-databend-client-caps") == "session_header", name
                            if index == 0:
                                assert "x-databend-session" not in headers, name
                            else:
                                assert headers.get("x-databend-session") == f"opaque-https-session-{index - 1}", name
                        else:
                            assert "x-databendcloud-token" not in headers, name
                            assert headers.get("authorization") == authorization, name
                            assert "cookie_enabled=true" in headers.get("cookie", ""), name
                            assert "x-databend-client-caps" not in headers, name
                            assert "x-databend-session" not in headers, name
                        if path == "/v1/query":
                            payload = json.loads(body)
                            assert "string_fields" not in payload, name
                            assert payload.get("pagination") == expected_pagination, name
                    if expected_cloud:
                        assert not any(path.startswith("/v1/session/login") for _, path, _, _ in server.records), name
                        assert server.tunnels, name + ": CLI did not use local CONNECT proxy"
                        assert all(target in {"api.databend.com:443", "api.databend.cn:443"}
                                   for target in server.tunnels), name
                        assert all(headers.get("host") in {"api.databend.com", "api.databend.com:443",
                                                           "api.databend.cn", "api.databend.cn:443"}
                                   for _, _, headers, _ in server.records), name
                        assert "2" in result.stdout.splitlines(), (name, text)
                        assert server.pages == 2, name + ": page 503 not retried"
                        logouts = [(method, headers, body) for method, path, headers, body in server.records
                                   if path == "/v1/session/logout"]
                        assert len(logouts) == int(keep_alive), name + ": conditional logout"
                        if keep_alive:
                            method, headers, body = logouts[0]
                            headers = {key.lower(): value for key, value in headers.items()}
                            assert method == "POST" and body == b"", name
                            assert headers.get("x-databend-sticky-node") == "sticky-node", name
                            assert server.records[-1][1] == "/v1/session/logout", name
                        assert any(method == "GET" and path.endswith("/final")
                                   for method, path, _, _ in server.records), name
                print("PASS:", name)

        try:
            env_credentials = {"BENDSQL_CLOUD_API_TOKEN": TOKEN}
            endpoint = "databend://root:%FF@api.databend.com:443/default?" + urlencode({
                "tls_ca_file": ca, "org": "org-example", "retry_delay_secs": "0"})
            run("Cloud DSN through HTTPS, pagination/session refresh/final", ["--dsn", cloud], expected_cloud=True)
            run("Cloud forces login disabled", ["--dsn", cloud + "&login=enable"], expected_cloud=True)
            pagination = {"wait_time_secs": 1, "max_rows_in_buffer": 1000, "max_rows_per_page": 1}
            pagination_query = urlencode(pagination)
            run("Cloud DSN forwards pagination ignored by current WebAPI", ["--dsn", cloud + "&" + pagination_query],
                expected_cloud=True, expected_pagination=pagination)
            run("Cloud forwards partial pagination including zero", ["--dsn", cloud + "&wait_time_secs=0"],
                expected_cloud=True, expected_pagination={"wait_time_secs": 0})
            run("Cloud Arrow remains unsupported with pagination", ["--dsn", cloud + "&" + pagination_query + "&query_result_format=arrow"],
                expected_error="does not support Arrow")
            run("Cloud sticky session logs out with latest header", ["--dsn", cloud], expected_cloud=True, keep_alive=True)
            run("missing session capability fails instead of Cookie fallback", ["--dsn", cloud],
                expected_cloud=True, expected_error="requires session_header support", emit_session_header=False)
            run("Cloud DSN with environment token", ["--dsn", endpoint], env_credentials, expected_cloud=True)
            run("Cloud BENDSQL_DSN with environment token", [], dict(env_credentials, BENDSQL_DSN=endpoint), expected_cloud=True)
            run("case-insensitive official host", ["--dsn", endpoint.replace("api.databend.com", "API.DATABEND.COM")], env_credentials, expected_cloud=True)
            run("Cloud DSN org override", ["--dsn", endpoint.replace("org=org-example", "org=other"), "--org", "org-example"], env_credentials, expected_cloud=True)
            run("header sessions ignore Set-Cookie", ["--dsn", cloud], expected_cloud=True, emit_session_cookies=False)
            run("China DSN", ["--dsn", cloud.replace("api.databend.com", "api.databend.cn")], expected_cloud=True)
            run("transaction pass-through", ["--dsn", cloud], expected_cloud=True, query="/* session */ BEGIN")
            cloud_flags = ["--org", "org-example", "--set", f"tls_ca_file={ca}", "--set", "retry_delay_secs=0"]
            run("org selects Cloud default target", cloud_flags, env_credentials, expected_cloud=True)
            pagination_flags = [value for key, number in pagination.items() for value in ("--set", f"{key}={number}")]
            run("Cloud --set forwards pagination", [*cloud_flags, *pagination_flags], env_credentials,
                expected_cloud=True, expected_pagination=pagination)
            run("org region override", [*cloud_flags, "--host", "api.databend.cn"], env_credentials, expected_cloud=True)
            run("explicit SQL user conflicts with Cloud", [*cloud_flags, "--user=root"], env_credentials, expected_error="cannot be combined with --user")
            run("explicit SQL password conflicts with Cloud", [*cloud_flags, "--password=ignored"], env_credentials, expected_error="cannot be combined with --password")
            run("explicit TLS disable fails", [*cloud_flags, "--tls=false"], env_credentials, expected_error="requires HTTPS")
            run("official host alone is not a selector", ["--host", "api.databend.com"], env_credentials, expected_error="requires org")
            run("explicit token alone is not a selector", ["--cloud-api-token=" + TOKEN], expected_error="requires org")
            no_ca = "databend://api.databend.com/default?" + urlencode({"cloud_api_token": TOKEN, "org": "org-example", "retry_delay_secs": "0"})
            run("TLS verifies the mock CA", ["--dsn", no_ca], expected_error="error sending request")
            for host in ["localhost:8443", "staging.example:8443", "api.databend.com:8443"]:
                run("untrusted origin rejected: " + host, ["--dsn", cloud.replace("api.databend.com:443", host)], expected_error="official HTTPS API host")
            basic = "Basic " + base64.b64encode(b"root:pw").decode()
            run("native DSN ignores ambient Cloud token", ["--dsn", private], env_credentials, authorization=basic)
            run("native pagination is unchanged", ["--dsn", private + "&" + pagination_query], env_credentials,
                authorization=basic, expected_pagination=pagination)
            run("native BENDSQL_DSN ignores ambient Cloud token", [], dict(env_credentials, BENDSQL_DSN=private), authorization=basic)
            run("native transactions unchanged", ["--dsn", private], authorization=basic, query="BEGIN")
            flags = ["--host=127.0.0.1", f"--port={server.server_port}", "--tls=true", "--set", f"tls_ca_file={ca}", "--set", "presign=off"]
            empty_basic = "Basic " + base64.b64encode(b"root:").decode()
            run("host flag plus environment token stays native", flags, env_credentials, authorization=empty_basic)
            run("private credentials plus environment token stay native", [*flags, "--user=root", "--password=pw"], env_credentials, authorization=basic)
            run("native Access Token stays native", [*flags, "--set", "access_token=native-fixture"], env_credentials, authorization="Bearer native-fixture")
            run("empty environment token is ignored in native mode", flags, {"BENDSQL_CLOUD_API_TOKEN": ""}, authorization=empty_basic)
            native_config = f"[connection]\nhost='127.0.0.1'\nport={server.server_port}\ntls=true\n[connection.args]\ntls_ca_file='{ca}'\npresign='off'\n"
            run("config host plus environment token stays native", [], env_credentials, config=native_config, authorization=empty_basic)
            private_config = "[connection]\nhost='localhost'\nport=8000\nuser='root'\ntls=false\n[connection.args]\naccess_token_file='missing-native-token'\nprivate_key_file='missing-native-key'\nsslmode='disable'\ntenant='tn-private'\n"
            for gateway_dsn in [private, "not a valid DSN"]:
                run("org overrides ambient private target with warnings", cloud_flags,
                    dict(env_credentials, BENDSQL_DSN=gateway_dsn, BENDSQL_PASSWORD="native-password"), config=private_config, expected_cloud=True)
            run("Cloud config is not supported", [], env_credentials, config="[connection.args]\norg='org-example'\n", expected_error="not supported in config [connection]")
        finally:
            for service in (server, proxy):
                service.shutdown()
                service.server_close()
            for thread in threads:
                thread.join(timeout=3)


if __name__ == "__main__":
    main()
