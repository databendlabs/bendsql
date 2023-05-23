# Copyright 2023 Datafuse Labs.
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
import os

from behave import given, when, then
from behave.api.async_step import async_run_until_complete
import _databend_python


@given("A new Databend-Python Async Connector")
@async_run_until_complete
async def step_impl(context):
    dsn = os.getenv("TEST_DATABEND_DSN", "databend+http://root:root@localhost:8000/?sslmode=disable")
    context.ad = _databend_python.AsyncDatabendDriver(dsn)


@when('Async exec "{create_sql}"')
@async_run_until_complete
async def step_impl(context, create_sql):
    await context.ad.exec(create_sql)


@when('Async exec "{insert_sql}"')
@async_run_until_complete
async def step_impl(context, insert_sql):
    await context.ad.exec(insert_sql)


@then('The select "{select_sql}" should run')
@async_run_until_complete
async def step_impl(context, select_sql):
    await context.ad.exec(select_sql)
