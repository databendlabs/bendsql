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

"""Credential-free tests of the live test harness; no subprocess or network calls."""

from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

import cloud_webapi_e2e as e2e

TOKEN = "fake-unit-cloud-token"
ORG = "org-private-unit-fixture"
ENV = {"BENDSQL_CLOUD_API_TOKEN": TOKEN, "BENDSQL_ORG": ORG}


class CloudE2EHarnessTests(unittest.TestCase):
    def runner(self, environment=None):
        return e2e.Runner("/fake/bendsql", ENV if environment is None else environment)

    def test_ci_context_requires_upstream_main_and_safe_event(self):
        context = dict(ENV, GITHUB_ACTIONS="true", GITHUB_REPOSITORY="databendlabs/bendsql",
                       GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch")
        for event in ["workflow_dispatch", "schedule"]:
            self.runner(dict(context, GITHUB_EVENT_NAME=event))
        for key, value in [("GITHUB_REPOSITORY", "someone/bendsql"),
                           ("GITHUB_REF", "refs/heads/unreviewed"),
                           ("GITHUB_REF", "refs/pull/811/merge"),
                           ("GITHUB_EVENT_NAME", "pull_request"),
                           ("GITHUB_EVENT_NAME", "pull_request_target"),
                           ("GITHUB_EVENT_NAME", "workflow_run")]:
            with self.subTest(key=key, value=value), self.assertRaises(e2e.E2EFailure):
                self.runner(dict(context, **{key: value}))

    def test_missing_or_empty_credentials_fail_without_execution(self):
        for environment in [{}, {"BENDSQL_CLOUD_API_TOKEN": TOKEN},
                            dict(ENV, BENDSQL_CLOUD_API_TOKEN=""), dict(ENV, BENDSQL_ORG=" ")]:
            with patch.object(e2e.subprocess, "run") as run, self.assertRaises(e2e.E2EFailure):
                self.runner(environment)
            run.assert_not_called()

    def test_child_environment_removes_connection_overrides_and_other_secrets(self):
        environment = dict(ENV, BENDSQL_DSN="untrusted-dsn", BENDSQL_HOST="evil.example",
                           BENDSQL_CLOUD_API_ORIGIN="https://evil.example", RUST_LOG="trace",
                           AWS_SECRET_ACCESS_KEY="not-inherited", HOME="/existing/config",
                           PATH="/bin", HTTPS_PROXY="http://proxy.example")
        child = e2e.child_environment(environment, "/fresh/home")
        self.assertEqual(child["HOME"], "/fresh/home")
        self.assertEqual(child["HTTPS_PROXY"], "http://proxy.example")
        self.assertEqual({key for key in child if key.startswith("BENDSQL_")}, set(ENV))
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", child)
        self.assertNotIn("RUST_LOG", child)

    def test_credentials_are_environment_only_and_warehouse_is_default(self):
        result = subprocess.CompletedProcess([], 0, "42\tcloud-e2e\n", "")
        with patch.object(e2e.subprocess, "run", return_value=result) as run:
            rows = self.runner().query("scalar", "SELECT 42, 'cloud-e2e'")
        self.assertEqual(rows, [["42", "cloud-e2e"]])
        command = run.call_args.args[0]
        self.assertNotIn(TOKEN, " ".join(command))
        self.assertNotIn(ORG, " ".join(command))
        self.assertIn("warehouse=default", command)
        self.assertIn("--log-level=off", command)
        self.assertEqual(run.call_args.kwargs["env"]["BENDSQL_CLOUD_API_TOKEN"], TOKEN)
        self.assertFalse(Path(run.call_args.kwargs["env"]["HOME"]).exists())
        self.assertEqual(run.call_args.kwargs["timeout"], 90)

    def test_failures_do_not_echo_server_or_credential_data(self):
        for result in [subprocess.CompletedProcess([], 1, "", f"401 {ORG} private account data"),
                       subprocess.CompletedProcess([], 1, "", TOKEN)]:
            with patch.object(e2e.subprocess, "run", return_value=result):
                with self.assertRaises(e2e.E2EFailure) as raised:
                    self.runner().query("scalar", "SELECT 42")
            message = str(raised.exception)
            self.assertNotIn(TOKEN, message)
            self.assertNotIn(ORG, message)
            self.assertNotIn("private account data", message)

    def test_network_failure_is_not_accepted_as_expected_sql_error(self):
        with patch.object(e2e.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "503")):
            with self.assertRaises(e2e.E2EFailure):
                self.runner().query("SQL-error", "SELECT cloud_e2e_missing_column",
                                    expected_error="cloud_e2e_missing_column")
        with patch.object(e2e.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 1, "", "unknown column cloud_e2e_missing_column")):
            self.runner().query("SQL-error", "SELECT cloud_e2e_missing_column",
                                expected_error="cloud_e2e_missing_column")

    def test_timeout_output_is_withheld(self):
        with patch.object(e2e.subprocess, "run", side_effect=subprocess.TimeoutExpired(
                ["bendsql"], 90, output=TOKEN, stderr=ORG)):
            with self.assertRaises(e2e.E2EFailure) as raised:
                self.runner().query("scalar", "SELECT 42")
        self.assertNotIn(TOKEN, str(raised.exception))
        self.assertNotIn(ORG, str(raised.exception))

    def test_suite_uses_read_only_sql_and_does_not_print_identity(self):
        runner = Mock()
        runner.query.side_effect = [[["42", "cloud-e2e"]], [["private-user@example", "public"]],
                                    [[str(index)] for index in range(5000)], []]
        output = io.StringIO()
        with redirect_stdout(output):
            e2e.run_suite(runner)
        self.assertNotIn("private-user@example", output.getvalue())
        for call in runner.query.call_args_list:
            self.assertTrue(call.args[1].startswith("SELECT "))
        self.assertEqual(runner.query.call_count, 4)

    def test_incomplete_result_fails_without_printing_rows(self):
        runner = Mock()
        runner.query.side_effect = [[["42", "cloud-e2e"]], [["private-user@example", "public"]],
                                    [[str(index)] for index in range(4999)]]
        with redirect_stdout(io.StringIO()), self.assertRaises(e2e.E2EFailure):
            e2e.run_suite(runner)

    def test_main_missing_secret_error_does_not_print_environment(self):
        output = io.StringIO()
        with patch.dict(os.environ, {"BENDSQL_CLOUD_API_TOKEN": TOKEN}, clear=True):
            with redirect_stderr(output):
                self.assertEqual(e2e.main(["/fake/bendsql"]), 1)
        self.assertNotIn(TOKEN, output.getvalue())


if __name__ == "__main__":
    unittest.main()
