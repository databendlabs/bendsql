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

"""Opt-in, read-only live Cloud smoke tests. See cli/tests/README.md.

Never print captured CLI output: it can contain account identity or server echoes.
"""

import csv
import io
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


class E2EFailure(Exception):
    pass


def validate_context(environment):
    if environment.get("GITHUB_ACTIONS") == "true" and (
        environment.get("GITHUB_REPOSITORY") != "databendlabs/bendsql"
        or environment.get("GITHUB_REF") != "refs/heads/main"
        or environment.get("GITHUB_EVENT_NAME") not in {"workflow_dispatch", "schedule"}
    ):
        raise E2EFailure("Live Cloud tests require manual/scheduled execution on upstream main")


def child_environment(environment, home):
    # Do not inherit DSNs, custom origins, config locations or other credentials.
    allowed = {"PATH", "LANG", "LC_ALL", "SYSTEMROOT", "SSL_CERT_FILE", "SSL_CERT_DIR",
               "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
               "http_proxy", "https_proxy", "all_proxy", "no_proxy"}
    result = {key: value for key, value in environment.items() if key in allowed}
    for key in ("BENDSQL_CLOUD_API_TOKEN", "BENDSQL_ORG"):
        value = environment.get(key, "")
        if not value.strip():
            raise E2EFailure("Required Cloud E2E Secrets are missing or empty")
        result[key] = value
    result.update(HOME=home, TERM="dumb", RUST_BACKTRACE="0")
    return result


class Runner:
    def __init__(self, binary, environment):
        validate_context(environment)
        self.binary = str(Path(binary).resolve())
        self.environment = child_environment(environment, "")

    def query(self, label, sql, expected_error=None):
        with tempfile.TemporaryDirectory(prefix="bendsql-cloud-e2e-") as home:
            environment = dict(self.environment, HOME=home)
            command = [self.binary, "--non-interactive", "--output=tsv", "--log-level=off",
                       "--set", "warehouse=default", "--set", "retry_count=3",
                       "--set", "login=disable",
                       f"--query={sql}"]
            try:
                result = subprocess.run(command, env=environment, stdin=subprocess.DEVNULL,
                                        capture_output=True, text=True, errors="replace", timeout=90)
            except subprocess.TimeoutExpired:
                raise E2EFailure(f"{label}: timed out after 90 seconds; output withheld") from None
            except OSError:
                raise E2EFailure(f"{label}: could not execute bendsql") from None
            text = result.stdout + result.stderr
            token = environment["BENDSQL_CLOUD_API_TOKEN"]
            if token in text:
                raise E2EFailure(f"{label}: credential echoed by CLI; output withheld")
            if len(text) > 1024 * 1024:
                raise E2EFailure(f"{label}: output exceeded smoke-test budget")
            if expected_error is not None:
                if result.returncode == 0 or expected_error not in result.stderr.lower():
                    raise E2EFailure(f"{label}: expected SQL error was not reported; output withheld")
                return []
            if result.returncode != 0:
                status = re.search(r"\b(401|403|404|408|429|500|502|503|504)\b", result.stderr)
                hint = f"; HTTP status {status[0]}" if status else ""
                raise E2EFailure(f"{label}: bendsql exited with code {result.returncode}{hint}; output withheld")
            try:
                return [row for row in csv.reader(io.StringIO(result.stdout), delimiter="\t") if row]
            except csv.Error:
                raise E2EFailure(f"{label}: invalid TSV output; output withheld") from None


def run_suite(runner):
    if runner.query("scalar", "SELECT 42, 'cloud-e2e'") != [["42", "cloud-e2e"]]:
        raise E2EFailure("scalar: unexpected result; output withheld")
    print("PASS: scalar result and string-field decoding")

    rows = runner.query("identity", "SELECT current_user(), current_role()")
    if len(rows) != 1 or len(rows[0]) != 2 or not all(value.strip() for value in rows[0]):
        raise E2EFailure("identity: missing server user/role; output withheld")
    print("PASS: server SQL identity (values withheld)")

    rows = runner.query("large-result", "SELECT number FROM numbers(5000) ORDER BY number")
    if len(rows) != 5000 or any(row != [str(index)] for index, row in enumerate(rows)):
        raise E2EFailure("large-result: incomplete or misordered results; output withheld")
    print("PASS: complete ordered result stream (5000 rows)")

    for ending in ("ROLLBACK", "COMMIT"):
        rows = runner.query("transaction-" + ending.lower(),
                            "BEGIN; SELECT 'transaction-ok'; " + ending)
        if rows != [["transaction-ok"]]:
            raise E2EFailure("transaction: incomplete result; output withheld")
    print("PASS: read-only transaction commit and rollback on one connection")

    runner.query("SQL-error", "SELECT cloud_e2e_missing_column", expected_error="cloud_e2e_missing_column")
    print("PASS: server SQL error propagation")


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if len(arguments) != 1:
        print("Usage: python3 cli/tests/cloud_webapi_e2e.py /path/to/bendsql", file=sys.stderr)
        return 2
    try:
        run_suite(Runner(arguments[0], os.environ))
    except E2EFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print("PASS: live Cloud WebAPI smoke tests on default Warehouse")
    return 0


if __name__ == "__main__":
    sys.exit(main())
