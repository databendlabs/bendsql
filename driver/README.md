# Databend Driver

Databend unified SQL client for RestAPI and FlightSQL

[![crates.io](https://img.shields.io/crates/v/databend-driver.svg)](https://crates.io/crates/databend-driver)
![License](https://img.shields.io/crates/l/databend-driver.svg)

## Usage

### Basic Operations

```rust
use databend_driver::Client;

let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
let client = Client::new(dsn);
let conn = client.get_conn().await.unwrap();

// Execute DDL
let sql_create = "CREATE TABLE books (
    title VARCHAR,
    author VARCHAR,
    date Date
);";
conn.exec(sql_create).await.unwrap();

// Execute DML
let sql_insert = "INSERT INTO books VALUES ('The Little Prince', 'Antoine de Saint-Exupéry', '1943-04-06');";
conn.exec(sql_insert).await.unwrap();

conn.close().await
```

### Query Operations

#### Query Single Row

```rust
// Simple query
let row = conn.query_row("SELECT * FROM books").await.unwrap();
let (title, author, date): (String, String, chrono::NaiveDate) = row.unwrap().try_into().unwrap();
println!("{} {} {}", title, author, date);

// Using Builder pattern for better flexibility
let row = conn.query("SELECT * FROM books WHERE title = ?")
    .bind(params!["The Little Prince"])
    .one()
    .await.unwrap();
```

#### Query Multiple Rows

```rust
// Get all rows
let rows = conn.query("SELECT * FROM books").all().await.unwrap();
for row in rows {
    let (title, author, date): (String, String, chrono::NaiveDate) = row.try_into().unwrap();
    println!("{} {} {}", title, author, date);
}

// Stream processing for large datasets
let mut iter = conn.query("SELECT * FROM books").iter().await.unwrap();
while let Some(row) = iter.next().await {
    let (title, author, date): (String, String, chrono::NaiveDate) = row.unwrap().try_into().unwrap();
    println!("{} {} {}", title, author, date);
}
```

### Parameter Bindings

The driver supports multiple parameter binding styles:

```rust
// Positional parameters (PostgreSQL style)
let row = conn.query("SELECT $1, $2, $3, $4")
    .bind((3, false, 4, "55"))
    .one()
    .await.unwrap();

// Named parameters
let params = params! {a => 3, b => false, c => 4, d => "55"};
let row = conn.query("SELECT :a, :b, :c, :d")
    .bind(params)
    .one()
    .await.unwrap();

// Question mark placeholders  
let row = conn.query("SELECT ?, ?, ?, ?")
    .bind((3, false, 4, "55"))
    .one()
    .await.unwrap();

// Insert with parameters
conn.exec("INSERT INTO books VALUES (?, ?, ?)")
    .bind(("New Book", "Author Name", "2024-01-01"))
    .await.unwrap();
```

### Builder Pattern API

The driver provides a flexible Builder pattern for complex queries:

```rust
// Simple execution
conn.exec("CREATE TABLE test (id INT)").await?;

// Conditional parameter binding
let mut query = conn.query("SELECT * FROM books");
if let Some(author_filter) = author_name {
    query = query.bind(params![author_filter]);
}
let books = query.all().await?;

// Different execution modes
let single_book = conn.query("SELECT * FROM books LIMIT 1").one().await?;
let all_books = conn.query("SELECT * FROM books").all().await?;
let book_stream = conn.query("SELECT * FROM books").iter().await?;
let book_stats = conn.query("SELECT COUNT(*) FROM books").iter_ext().await?;
```

### ORM Support

```rust
use databend_driver::serde_bend;

#[derive(serde_bend, Debug)]
struct Book {
    title: String,
    author: String,
    #[serde_bend(rename = "publication_date")]
    date: chrono::NaiveDate,
}

// Query as typed objects
let cursor = conn.query_as::<Book>("SELECT * FROM books WHERE author = ?")
    .bind(params!["Antoine de Saint-Exupéry"])
    .await?;

let books = cursor.fetch_all().await?;
for book in books {
    println!("{:?}", book);
}

// Insert typed objects  
let mut insert = conn.insert::<Book>("books").await?;
insert.write(&Book {
    title: "New Book".to_string(),
    author: "New Author".to_string(), 
    date: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
}).await?;
let inserted = insert.end().await?;
```

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend    | Rust                    |
| ----------- | ----------------------- |
| `BOOLEAN`   | `bool`                  |
| `TINYINT`   | `i8`,`u8`               |
| `SMALLINT`  | `i16`,`u16`             |
| `INT`       | `i32`,`u32`             |
| `BIGINT`    | `i64`,`u64`             |
| `FLOAT`     | `f32`                   |
| `DOUBLE`    | `f64`                   |
| `DECIMAL`   | `String`                |
| `DATE`      | `chrono::NaiveDate`     |
| `TIMESTAMP` | `chrono::NaiveDateTime` |
| `VARCHAR`   | `String`                |
| `BINARY`    | `Vec<u8>`               |

### Semi-Structured Data Types

| Databend      | Rust            |
| ------------- | --------------- |
| `ARRAY[T]`    | `Vec<T>`        |
| `TUPLE[T, U]` | `(T, U)`        |
| `MAP[K, V]`   | `HashMap<K, V>` |
| `VARIANT`     | `String`        |
| `BITMAP`      | `String`        |
| `GEOMETRY`    | `String`        |
| `GEOGRAPHY`   | `String`        |

Note: `VARIANT` is a json encoded string. Example:

```sql
CREATE TABLE example (
    data VARIANT
);
INSERT INTO example VALUES ('{"a": 1, "b": "hello"}');
```

```rust
let row = conn.query_row("SELECT * FROM example LIMIT 1").await.unwrap();
let (data,): (String,) = row.unwrap().try_into().unwrap();
let value: serde_json::Value = serde_json::from_str(&data).unwrap();
println!("{:?}", value);
```

## Migration from Old API

If you're upgrading from an older version, here's how to migrate:

```rust
// Old API
conn.exec("INSERT INTO test VALUES (?)", params![1]).await?;
conn.query_all("SELECT * FROM test WHERE id = ?", params![1]).await?;

// New Builder API (recommended)
conn.exec("INSERT INTO test VALUES (?)").bind(params![1]).await?;
conn.query("SELECT * FROM test WHERE id = ?").bind(params![1]).all().await?;

// Or use direct methods (compatible)
conn.query_all("SELECT * FROM test").await?;
```
