# Databend Driver

Databend Driver for Rust

[![crates.io](https://img.shields.io/crates/v/databend-driver.svg)](https://crates.io/crates/databend-driver)
![License](https://img.shields.io/crates/l/databend-driver.svg)

## usage


### exec

```rust
let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
let conn = DatabendConnection::create(dsn).unwrap();

let sql_create = "CREATE TABLE books (
    title VARCHAR,
    author VARCHAR,
    date Date
);";
conn.exec(&sql_create).await.unwrap();
let sql_insert = "INSERT INTO books VALUES ('The Little Prince', 'Antoine de Saint-Exupéry', '1943-04-06');";
conn.exec(&sql_insert).await.unwrap();
```

### query row

```rust
let sql_select = "SELECT * FROM books;";
let row = conn.query_row(&sql_select).await.unwrap();
let (title,author,date): (String,String,i32) = row.try_into().unwrap();
```

### query iter

```rust
let sql_select = "SELECT * FROM books;";
let mut rows = conn.query_iter(&sql_select).await.unwrap();
while let Some(row) = rows.next().await {
    let row = row.unwrap();
    let (title,author,date): (String,String,chrono::NaiveDate) = row.try_into().unwrap();
}
```
