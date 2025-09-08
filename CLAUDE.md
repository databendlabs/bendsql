# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

BendSQL is Databend's native client suite written in Rust with multiple language bindings. It provides both RestAPI and FlightSQL clients for connecting to Databend databases.

## Rules for LLM using this file

- Code comments should always use English
- Task summary or usage markdown files could be written into `${project_dir}/tmp`, it has been ignored by git

## Architecture

### Core Components

**Rust Workspace Structure:**
- `core/` - RestAPI client foundation (`databend-client` crate)
- `sql/` - SQL processing and row handling (`databend-driver-core` crate)
- `driver/` - Unified SQL client for RestAPI and FlightSQL (`databend-driver` crate)
- `macros/` - Procedural macros for ORM functionality (`databend-driver-macros` crate)
- `cli/` - Interactive CLI tool (`bendsql` binary)

**Language Bindings:**
- `bindings/python/` - Python client with ORM support
- `bindings/nodejs/` - Node.js client
- `bindings/java/` - Java client (upcoming)

### Key Architectural Patterns

**Driver Layer:** The `driver/` crate provides a unified interface over both RestAPI (`core/`) and FlightSQL protocols, with automatic protocol selection based on DSN.

**ORM System:** Both Rust and Python implementations use annotation-based field mapping:
- Rust: `#[serde_bend(rename = "field")]` macro attributes
- Python: `Annotated[str, rename("field")]` type annotations
- Supports field renaming, skip serialization/deserialization

**Type System:** Comprehensive mapping between Databend SQL types and native language types, with special handling for semi-structured data (VARIANT, ARRAY, MAP).

## Development Commands

### Building and Testing

```bash
# Format, lint, and security checks
make check

# Build with frontend (CLI includes React dashboard)
make build

# Run unit tests only
make test

# Run integration tests (requires Docker)
make integration

# Specific integration tests
make integration-driver
make integration-python
make integration-nodejs
```

### Python Development

```bash
cd bindings/python

# Set up development environment
uv sync
source .venv/bin/activate

# Build Python extension
maturin develop --uv

# Run behavior tests
behave tests/asyncio
behave tests/blocking
behave tests/cursor

# Run pytest tests (for ORM functionality)
python -m pytest tests/test_orm.py -v
python -m pytest tests/test_orm_integration.py -v -k "not real_connection"
```

### Testing Against Live Database

Integration tests expect Databend running on:
- Port 8000 for HTTP/RestAPI
- Port 8900 for FlightSQL
- Port 8000 for Python integration tests (recent addition)

Start test environment:
```bash
cd tests && make up
```

### Single Test Execution

```bash
# Rust driver tests
cargo test --test driver -- specific_test_name

# Python ORM tests
cd bindings/python
python -m pytest tests/test_orm.py::TestClass::test_method -v

# Run single behavior test
behave tests/asyncio --name="specific scenario"
```

## Important Implementation Details

### DSN Format
```
databend[+flight]://user:[password]@host[:port]/[database][?sslmode=disable][&arg1=value1]
```

Protocol selection is automatic - `+flight` suffix forces FlightSQL.

### Python ORM Usage
The Python binding includes a custom ORM system:

```python
from databend_driver import databend_model, rename, skip_serializing

@databend_model
@dataclass
class User:
    id: int
    username: Annotated[str, rename("user_name")]
    created_at: Annotated[Optional[datetime], skip_serializing()] = None
```

### Rust ORM Usage
```rust
#[derive(serde_bend, Debug, Clone)]
struct User {
    id: i32,
    #[serde_bend(rename = "user_name")]
    username: String,
    #[serde_bend(skip_serializing)]
    created_at: Option<NaiveDateTime>,
}
```

### Connection Patterns

**Rust:**
```rust
let client = Client::new("databend://root:@localhost:8000/default?sslmode=disable");
let conn = client.get_conn().await?;
```

**Python Async:**
```python
client = AsyncDatabendClient("databend://root:@localhost:8000/?sslmode=disable")
conn = await client.get_conn()
```

**Python Blocking:**
```python
client = BlockingDatabendClient("databend://root:@localhost:8000/?sslmode=disable")
conn = client.get_conn()
```

## Frontend Integration

The CLI includes a React-based performance analysis dashboard in `frontend/`. Build requires:
- pnpm for package management
- Frontend builds into `cli/frontend/build/`
- Integrated into Rust binary via build.rs

## Testing Philosophy

- **Unit tests:** Cover individual component logic
- **Integration tests:** Require live Databend instance via Docker Compose
- **Behavior tests:** Use Gherkin scenarios for Python bindings
- **Real connection tests:** Marked separately and skipped when DB unavailable

## Version Management

Workspace uses unified versioning across all crates via `workspace.package.version` in root `Cargo.toml`. Current version: 0.28.0.

All bindings (Python, Node.js) should maintain version parity with the Rust workspace.