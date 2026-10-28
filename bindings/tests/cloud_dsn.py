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

"""Test freshly built bindings through a local HTTPS mock, without Docker/secrets.

Only current wheel/addon artifacts run this suite, not the old-driver BDD matrix.
Workers receive DSNs via stdin. All proxy traffic stays on loopback; the mock
never forwards traffic, changes DNS/hosts, or bypasses the production allowlist.
"""

import argparse
import base64
import http.server
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "cli/tests"))
from cloud_webapi_https import Handler, certificates  # noqa: E402

TOKEN = 'fake-binding-token+/=&?%"'
ORG = "org-example"


def require(condition, message):
    # Never put headers, DSNs, session values or child diagnostics in failures.
    if not condition:
        raise AssertionError(message)


def run_suite(executable, binding):
    modes = ("blocking", "asyncio", "cursor") if binding == "python" else ("nodejs",)
    worker = ROOT / (
        "bindings/python/tests/cloud/dsn.py"
        if binding == "python"
        else "bindings/nodejs/tests/cloud/dsn.js"
    )
    with tempfile.TemporaryDirectory(prefix="binding-cloud-") as temporary:
        directory = Path(temporary)
        certificates(directory)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(directory / "server.pem", directory / "server.key")
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.state = server
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        proxy.context, proxy.state = context, server
        services = (server, proxy)
        threads = [
            threading.Thread(target=service.serve_forever, daemon=True)
            for service in services
        ]
        for thread in threads:
            thread.start()

        options = {
            "tls_ca_file": str(directory / "ca.pem"),
            "cloud_api_token": TOKEN,
            "org": ORG,
            "warehouse": "default",
            "retry_delay_secs": "0",
        }

        def dsn(host="api.databend.com", scheme="databend", extra=None, omit=()):
            query = {key: value for key, value in options.items() if key not in omit}
            query.update(extra or {})
            return f"{scheme}://{host}/default?" + urlencode(query)

        # Explicit BENDSQL_* credentials must not become Driver defaults.
        environment = {
            key: value
            for key, value in os.environ.items()
            if key
            in {"PATH", "HOME", "SYSTEMROOT", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH"}
        }
        environment.update(
            HTTPS_PROXY=f"http://127.0.0.1:{proxy.server_port}",
            HTTP_PROXY="http://127.0.0.1:0",
            ALL_PROXY="http://127.0.0.1:0",
            NO_PROXY="127.0.0.1,localhost",
            RUST_LOG="off",
            BENDSQL_CLOUD_API_TOKEN="ambient-not-a-driver-token",
        )
        private = (
            "databend://root:pw@127.0.0.1:"
            + str(server.server_port)
            + "/default?"
            + urlencode(
                {
                    "tls_ca_file": options["tls_ca_file"],
                    "presign": "off",
                    "retry_delay_secs": "0",
                }
            )
        )
        cases = [
            (
                "global and ignored SQL userinfo",
                dsn("ignored:%FF@api.databend.com"),
                True,
                None,
            ),
            ("China", dsn("api.databend.cn"), True, None),
            (
                "backend without session_header",
                dsn(),
                True,
                "requires session_header support",
            ),
            ("case-insensitive host", dsn("API.DATABEND.COM"), True, None),
            ("default host", dsn(""), True, None),
            ("private auth ignores Cloud environment", private, False, None),
            (
                "no implicit token from environment",
                dsn(omit=("cloud_api_token",)),
                True,
                "requires cloud_api_token",
            ),
            (
                "missing Organization ID",
                dsn(omit=("org",)),
                True,
                "requires org",
            ),
            (
                "empty Organization ID",
                dsn(extra={"org": ""}),
                True,
                "valid Organization ID",
            ),
            (
                "whitespace Organization ID",
                dsn(extra={"org": " "}),
                True,
                "valid Organization ID",
            ),
            ("explicit HTTP", dsn(scheme="databend+http"), True, "requires HTTPS"),
            ("TLS disabled", dsn(extra={"sslmode": "disable"}), True, "requires HTTPS"),
            (
                "FlightSQL",
                dsn(scheme="databend+flight"),
                True,
                "not supported with FlightSQL",
            ),
            ("gRPC", dsn(scheme="databend+grpc"), True, "not supported with FlightSQL"),
            (
                "foreign host",
                dsn("api.databend.com.evil.example"),
                True,
                "not a Gateway or private server",
            ),
            (
                "nonstandard port",
                dsn("api.databend.com:8443"),
                True,
                "not a Gateway or private server",
            ),
            (
                "competing Access Token",
                dsn(extra={"access_token": "native-fake"}),
                True,
                "cannot be combined",
            ),
            (
                "competing Key Pair",
                dsn(extra={"private_key_file": "missing-key"}),
                True,
                "cannot be combined",
            ),
            (
                "separate tenant",
                dsn(extra={"tenant": "private-tenant"}),
                True,
                "org, not tenant",
            ),
            (
                "duplicate token",
                dsn() + "&cloud_api_token=other",
                True,
                "specified only once",
            ),
            ("duplicate org", dsn() + "&org=other", True, "specified only once"),
            (
                "old org parameter rejected",
                dsn(extra={"org_slug": "old-org"}),
                True,
                "use org (Organization ID)",
            ),
        ]
        try:
            for mode in modes:
                for name, connection_dsn, cloud, error in cases:
                    label = f"{mode}: {name}"
                    missing_session = name == "backend without session_header"
                    server.emit_session_header = not missing_session
                    server.records, server.pages, server.tunnels = [], 0, []
                    (
                        server.cloud,
                        server.emit_session_cookies,
                        server.session_version,
                    ) = cloud, True, 0
                    payload = {
                        "mode": mode,
                        "dsn": connection_dsn,
                        "error": error,
                        "expected": [1, 2] if cloud else [1],
                    }
                    result = subprocess.run(
                        [executable, str(worker)],
                        input=json.dumps(payload),
                        env=environment,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    require(
                        all(
                            value not in result.stdout + result.stderr
                            for value in (TOKEN, json.dumps(TOKEN)[1:-1])
                        ),
                        label + ": token in child output",
                    )
                    require(
                        "opaque-https-session-" not in result.stdout + result.stderr,
                        label + ": session in child output",
                    )
                    require(
                        result.returncode == 0 and result.stdout.strip() == "PASS",
                        label + ": binding worker failed (diagnostics withheld)",
                    )
                    if error:
                        if missing_session:
                            require(
                                bool(server.tunnels),
                                label + ": missing HTTPS connection",
                            )
                            require(
                                len(server.records) == 2
                                and server.records[0][1].startswith("/v1/session/login")
                                and server.records[1][0:2] == ("POST", "/v1/query"),
                                label
                                + ": unsupported backend must fail on first query without replay/polling",
                            )
                            require(
                                all(
                                    not any(
                                        key.lower() in {"cookie", "authorization"}
                                        for key in headers
                                    )
                                    for _, _, headers, _ in server.records
                                ),
                                label + ": Cookie/auth fallback",
                            )
                        else:
                            require(
                                not server.records and not server.tunnels,
                                label + ": network before validation",
                            )
                    else:
                        require(len(server.records) > 1, label + ": no query requests")
                        queries = 0
                        for index, (method, path, headers, body) in enumerate(
                            server.records
                        ):
                            headers = {
                                key.lower(): value for key, value in headers.items()
                            }
                            require(
                                f"databend-driver-{binding}"
                                in headers.get("user-agent", ""),
                                label + ": wrong binding user agent",
                            )
                            if cloud:
                                require(
                                    headers.get("x-databendcloud-token") == TOKEN,
                                    label + ": token DSN decoding",
                                )
                                require(
                                    headers.get("x-databendcloud-org") == ORG,
                                    label + ": org from DSN",
                                )
                                require(
                                    headers.get("x-databendcloud-warehouse")
                                    == "default",
                                    label + ": warehouse",
                                )
                                require(
                                    "authorization" not in headers
                                    and "cookie" not in headers,
                                    label + ": competing credentials sent",
                                )
                                require(
                                    headers.get("x-databend-client-caps")
                                    == "session_header",
                                    label + ": capability",
                                )
                                require(
                                    headers.get("x-databend-session")
                                    == (
                                        None
                                        if path.startswith("/v1/session/login")
                                        else f"opaque-https-session-{index - 1}"
                                    ),
                                    label + ": session round-trip/refresh",
                                )
                            else:
                                basic = "Basic " + base64.b64encode(b"root:pw").decode()
                                require(
                                    headers.get("authorization") == basic,
                                    label + ": private auth changed",
                                )
                                require(
                                    "cookie_enabled=true" in headers.get("cookie", ""),
                                    label + ": native Cookie missing",
                                )
                                require(
                                    not any(
                                        key in headers
                                        for key in (
                                            "x-databendcloud-token",
                                            "x-databendcloud-org",
                                            "x-databend-client-caps",
                                            "x-databend-session",
                                        )
                                    ),
                                    label + ": Cloud state in native request",
                                )
                            if path == "/v1/query":
                                queries += 1
                                require(
                                    method == "POST"
                                    and json.loads(body)["sql"] == "SELECT 1",
                                    label + ": query body",
                                )
                        require(
                            queries == 4, label + ": two queries on each connection"
                        )
                        if cloud:
                            require(
                                server.tunnels and server.pages == 5,
                                label + ": pagination/retry missing",
                            )
                            require(
                                sum(
                                    method == "GET" and path.endswith("/final")
                                    for method, path, _, _ in server.records
                                )
                                == 4,
                                label + ": GET final missing",
                            )
                    print("PASS:", label, flush=True)
        finally:
            for service in services:
                service.shutdown()
                service.server_close()
            for thread in threads:
                thread.join(timeout=3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--python", help="Python interpreter containing the freshly built wheel"
    )
    group.add_argument(
        "--node", help="Node executable; build the local native addon first"
    )
    args = parser.parse_args()
    executable = shutil.which(args.python or args.node)
    if not executable:
        parser.error("binding interpreter was not found")
    # Do not resolve symlinks: a venv's Python path selects its installed wheel.
    run_suite(os.path.abspath(executable), "python" if args.python else "nodejs")


if __name__ == "__main__":
    main()
