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

import nox
import os


@nox.session
@nox.parametrize("db_version", ["1.2.803", "1.2.791"])
def new_driver_with_old_servers(session, db_version):
    query_version = f"v{db_version}-nightly"
    session.install("behave")
    # cd bindings/python
    # maturin build --out dist
    d = "../../bindings/python/dist/"
    wheels = list(os.listdir(d))
    assert len(wheels) == 1
    for p in wheels:
        session.install(d + p)
    with session.chdir(".."):
        env = {
            "DATABEND_QUERY_VERSION": query_version,
            "DATABEND_META_VERSION": query_version,
            "DB_VERSION": db_version,
        }
        session.run("make", "test-bindings-python", env=env)
        session.run("make", "down")


# to avoid fail the compact test in repo databend
@nox.session
@nox.parametrize("driver_version", ["0.28.2", "0.28.1"])
def new_test_with_old_drivers(session, driver_version):
    session.install("behave")
    session.install(f"databend-driver=={driver_version}")
    with session.chdir(".."):
        env = {
            "DRIVER_VERSION": driver_version,
        }
        session.run("make", "test-bindings-python", env=env)
        session.run("make", "down")
