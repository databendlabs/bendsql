// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_driver::Client;
use std::time::Duration;
use tokio::time::sleep;

use crate::common::DEFAULT_DSN;

async fn test_temp_table(session_token_enabled: bool) {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    if dsn.starts_with("databend+flight://") {
        return;
    }

    let session_token = if session_token_enabled {
        "enable"
    } else {
        "disable"
    };
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let dsn = format!("{}&session_token={}", dsn, session_token);
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let row = conn.query_row("select version()").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    println!("version = {}", val);

    let _ = conn.exec("create temp table t1 (a int)").await.unwrap();
    let n = conn.exec("insert into t1 values (1),(2)").await.unwrap();
    assert_eq!(n, 2);

    let row = conn.query_row("select count(*) from t1").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (i64,) = row.try_into().unwrap();
    assert_eq!(val, 2);
    drop(conn);
    sleep(Duration::from_millis(100)).await;
}
#[tokio::test]
async fn test_temp_table_session_token() {
    test_temp_table(true).await;
}

#[tokio::test]
async fn test_temp_table_password() {
    test_temp_table(false).await;
}
