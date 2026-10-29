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

"""Worker for bindings/tests/cloud_dsn.py; uses the installed native extension."""

import asyncio
import json
import sys
from urllib.parse import parse_qs, urlsplit

import databend_driver


def require(condition, message):
    if not condition:
        raise AssertionError(message)


async def check_async(case):
    client = databend_driver.AsyncDatabendClient(case["dsn"])
    for _ in range(2):
        conn = await client.get_conn()
        try:
            for _ in range(2):
                rows = await conn.query_all("SELECT 1")
                require(
                    [row.values()[0] for row in rows] == case["expected"],
                    "incomplete rows",
                )
        finally:
            await conn.close()


def check_blocking(case):
    client = databend_driver.BlockingDatabendClient(case["dsn"])
    for _ in range(2):
        if case["mode"] == "cursor":
            cursor = client.cursor()
            try:
                for _ in range(2):
                    cursor.execute("SELECT 1")
                    require(
                        [row.values()[0] for row in cursor.fetchall()]
                        == case["expected"],
                        "incomplete cursor rows",
                    )
            finally:
                cursor.close()
        else:
            conn = client.get_conn()
            try:
                for _ in range(2):
                    rows = conn.query_all("SELECT 1")
                    require(
                        [row.values()[0] for row in rows] == case["expected"],
                        "incomplete rows",
                    )
            finally:
                conn.close()


def main():
    case = json.load(sys.stdin)
    try:
        if case["mode"] == "asyncio":
            asyncio.run(check_async(case))
        else:
            check_blocking(case)
    except databend_driver.Error as error:
        message = str(error)
        require(case["error"] and case["error"] in message, "unexpected Driver error")
        tokens = parse_qs(urlsplit(case["dsn"]).query).get("cloud_api_token", [])
        require(all(token not in message for token in tokens), "token in Driver error")
    else:
        require(not case["error"], "expected Driver error was not raised")
    print("PASS")


if __name__ == "__main__":
    # Deliberately suppress exception chaining/tracebacks containing child state.
    try:
        main()
    except Exception:
        sys.exit("FAIL: Python Cloud DSN assertion (diagnostics withheld)")
