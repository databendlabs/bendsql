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

use crate::common::DEFAULT_DSN;

#[tokio::test]
async fn set_timezone() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let row = conn.query_row("select timezone()", ()).await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "UTC");

    conn.exec("set timezone='Europe/London'", ()).await.unwrap();
    let row = conn.query_row("select timezone()", ()).await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "Europe/London");
}

#[tokio::test]
async fn set_timezone_with_dsn() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    if dsn.starts_with("databend+flight://") {
        // skip dsn variable test for flight
        return;
    }
    let client = Client::new(format!("{}&timezone=Europe/London", dsn));
    let conn = client.get_conn().await.unwrap();

    let row = conn.query_row("select timezone()", ()).await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "Europe/London");
}

#[tokio::test]
async fn change_password() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    if dsn.starts_with("databend+flight://") {
        return;
    }
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();
    let n = conn.exec("drop user if exists u1 ", ()).await.unwrap();
    assert_eq!(n, 0);
    let n = conn
        .exec("create user u1 identified by 'p1' ", ())
        .await
        .unwrap();
    assert_eq!(n, 0);

    let dsn = "databend://u1:p1@localhost:8000/default?sslmode=disable&session_token=enable";
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let n = conn
        .exec("alter user u1 identified by 'p2' ", ())
        .await
        .unwrap();
    assert_eq!(n, 0);

    let row = conn.query_row("select 1", ()).await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (i64,) = row.try_into().unwrap();
    assert_eq!(val, 1);
}
