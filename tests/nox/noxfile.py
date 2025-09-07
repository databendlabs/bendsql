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

@nox.session
@nox.parametrize("db_version",  ["1.2.803", "1.2.791"])
@nox.parametrize("driver_version", ["0.28.2", "0.28.1"])
def python_client(session, driver_version, db_version):
    query_version = f"v{db_version}-nightly"
    session.install("behave")
    session.install(f"databend-driver=={driver_version}")
    with session.chdir(".."):
        env = {
            "DATABEND_QUERY_VERSION": query_version,
            "DATABEND_META_VERSION": query_version,
            "DB_VERSION": db_version,
            "DRIVER_VERSION": driver_version,
        }
        session.run("make", "test-bindings-python", env=env)
        session.run("make", "down")
