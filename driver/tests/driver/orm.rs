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

use chrono::{NaiveDate, NaiveDateTime};
use databend_driver::serde_bend;

use databend_driver::{Client, Connection};
use std::assert_eq;

use crate::common::DEFAULT_DSN;

async fn prepare() -> Connection {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = Client::new(dsn.to_string());
    client.get_conn().await.unwrap()
}

#[derive(serde_bend, Clone, Debug, PartialEq, Default)]
struct UserRow {
    id: i32,
    #[serde_bend(rename = "user_name")]
    username: String,
    email: String,
    dt: NaiveDate,

    #[serde_bend(skip_serializing)]
    created_at: NaiveDateTime,
    #[serde_bend(skip_serializing)]
    value: String,

    #[serde_bend(skip_serializing, skip_deserializing)]
    unknown: String,
}

#[tokio::test]
async fn test_orm() -> databend_driver::Result<()> {
    let connection = prepare().await;

    let _ = connection
        .exec(
            "CREATE OR REPLACE TABLE users (
                id INT NOT NULL, 
                user_name STRING NOT NULL, 
                email STRING NOT NULL, 
                dt Date NOT NULL,
                created_at timestamp default now(),
                value String default 'abc'
            )",
        )
        .await
        .unwrap();

    let test_users = vec![
        UserRow {
            id: 1,
            username: "alice".to_string(),
            email: "alice@example.com".to_string(),
            dt: NaiveDate::from_ymd_opt(2011, 3, 6).unwrap(),
            ..Default::default()
        },
        UserRow {
            id: 2,
            username: "bob".to_string(),
            email: "bob@example.com".to_string(),
            dt: NaiveDate::from_ymd_opt(2011, 3, 7).unwrap(),
            ..Default::default()
        },
        UserRow {
            id: 3,
            username: "charlie".to_string(),
            email: "charlie@example.com".to_string(),
            dt: NaiveDate::from_ymd_opt(2011, 3, 8).unwrap(),
            ..Default::default()
        },
        UserRow {
            id: 4,
            username: "diana".to_string(),
            email: "diana@example.com".to_string(),
            dt: NaiveDate::from_ymd_opt(2011, 3, 9).unwrap(),
            ..Default::default()
        },
    ];

    let mut insert = connection.insert::<UserRow>("users").await?;

    for user in &test_users {
        insert.write(user).await?;
    }

    let rows_inserted = insert.end().await?;
    assert_eq!(
        rows_inserted,
        test_users.len() as i64,
        "Should insert {} rows",
        test_users.len()
    );
    let cursor = connection
        .query_as::<UserRow>("SELECT * FROM users ORDER BY id")
        .await?;

    let retrieved_users = cursor.fetch_all().await?;

    assert_eq!(
        retrieved_users.len(),
        test_users.len(),
        "Retrieved {} users, expected {}",
        retrieved_users.len(),
        test_users.len()
    );

    for (expected, actual) in test_users.iter().zip(retrieved_users.iter()) {
        let mut expected_for_comparison = expected.clone();
        expected_for_comparison.created_at = actual.created_at;
        expected_for_comparison.value = actual.value.clone();

        assert_eq!(
            *actual, expected_for_comparison,
            "Complete user data mismatch"
        );
    }

    let cursor = connection
        .query_as::<UserRow>("SELECT * FROM users WHERE id = 2")
        .await?;

    let specific_users = cursor.fetch_all().await?;
    assert_eq!(specific_users.len(), 1);
    assert_eq!(specific_users[0].id, 2);
    assert_eq!(specific_users[0].username, "bob");
    assert_eq!(specific_users[0].email, "bob@example.com");

    Ok(())
}

#[test]
fn test_usage_patterns() {
    assert_eq!(
        UserRow::field_names(),
        vec!["id", "user_name", "email", "dt"]
    );

    let user = UserRow {
        id: 123,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        dt: NaiveDate::from_ymd_opt(2011, 3, 6).unwrap(),
        ..Default::default()
    };

    let values = user.to_values();
    assert_eq!(values.len(), 4);
}

#[allow(dead_code)]
#[derive(serde_bend, Debug, Clone, Default)]
struct TestFieldExclusionStruct {
    id: i32,
    username: String,

    #[serde_bend(skip_serializing)]
    created_at: String,

    #[serde_bend(skip_deserializing)]
    password: String,

    #[serde_bend(skip_serializing, skip_deserializing)]
    internal_field: String,
}

#[test]
fn test_comprehensive_field_exclusion() {
    // Test query field names (exclude skip_deserializing and skip_both)
    let query_fields = TestFieldExclusionStruct::query_field_names();
    assert_eq!(query_fields, vec!["id", "username", "created_at"]);

    // Test insert field names (exclude skip_serializing and skip_both)
    let insert_fields = TestFieldExclusionStruct::insert_field_names();
    assert_eq!(insert_fields, vec!["id", "username", "password"]);

    // Test backward compatibility
    let default_fields = TestFieldExclusionStruct::field_names();
    assert_eq!(default_fields, vec!["id", "username", "password"]);
}

#[test]
fn test_field_exclusion() {
    // Test that query fields exclude skip_deserializing fields
    // UserRow has: created_at (skip_serializing), value (skip_serializing), unknown (skip_both)
    // Query should include: id, user_name, email, dt, created_at, value (everything except skip_deserializing/skip_both)
    let query_fields = UserRow::query_field_names();
    assert_eq!(
        query_fields,
        vec!["id", "user_name", "email", "dt", "created_at", "value"]
    );

    // Test that insert fields exclude skip_serializing fields
    // Insert should exclude: created_at, value, unknown (all skip_serializing fields)
    let insert_fields = UserRow::insert_field_names();
    assert_eq!(insert_fields, vec!["id", "user_name", "email", "dt"]);

    // For backward compatibility, field_names should match insert_fields
    let default_fields = UserRow::field_names();
    assert_eq!(default_fields, insert_fields);
}
