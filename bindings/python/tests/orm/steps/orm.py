"""
Step definitions for Databend ORM functionality tests
"""

import asyncio
from dataclasses import dataclass
from typing import Annotated, Optional
from datetime import datetime, date
from behave import given, when, then
from behave.runner import Context

from databend_driver.orm import (
    databend_model, rename, skip_serializing, skip_deserializing, 
    skip_both, Rename, SkipSer, SkipDeser, Skip, ORMError
)


# Test models defined at module level so they can be reused
@databend_model
@dataclass
class SimpleModel:
    """Simple model without annotations"""
    id: int
    name: str
    active: bool = True


@databend_model
@dataclass
class UserModel:
    """User model with field renaming"""
    id: int
    username: Annotated[str, rename("user_name")]
    email: str
    birth_date: date


@databend_model
@dataclass
class SkipModel:
    """Model with skip_serializing fields"""
    id: int
    name: str
    created_at: Annotated[Optional[datetime], skip_serializing()] = None
    value: Annotated[str, skip_serializing()] = "default"


@databend_model
@dataclass
class DeserializeModel:
    """Model for testing deserialization"""
    id: int
    username: Annotated[str, rename("user_name")]
    email: str
    birth_date: date
    created_at: Annotated[Optional[datetime], skip_serializing()] = None
    value: Annotated[str, skip_serializing()] = "default"
    internal_field: Annotated[str, skip_both()] = ""


@databend_model
@dataclass
class AliasModel:
    """Model using convenience aliases"""
    id: int
    name: Annotated[str, Rename("full_name")]
    created: Annotated[Optional[datetime], SkipSer] = None
    secret: Annotated[str, Skip] = "secret"


@databend_model
@dataclass
class ComplexModel:
    """Complex model with mixed annotations"""
    id: int
    name: Annotated[str, rename("full_name")]
    email: str
    created: Annotated[Optional[datetime], skip_serializing()] = None
    updated: Annotated[Optional[datetime], skip_deserializing()] = None
    secret: Annotated[str, skip_both()] = "secret"


class MockConnection:
    """Mock database connection for testing"""
    
    def __init__(self):
        self.executed_sqls = []
        self.executed_params = []
    
    async def exec(self, sql, params):
        self.executed_sqls.append(sql)
        self.executed_params.append(params)
        return 1
    
    async def query_iter(self, sql, params):
        # Mock async iterator
        class AsyncRowIter:
            def __init__(self):
                self.rows = [
                    [1, "alice", "alice@example.com", date(1990, 1, 15)],
                    [2, "bob", "bob@example.com", date(1985, 3, 22)]
                ]
                self.index = 0
            
            def __aiter__(self):
                return self
            
            async def __anext__(self):
                if self.index >= len(self.rows):
                    raise StopAsyncIteration
                row = self.rows[self.index]
                self.index += 1
                return row
        
        return AsyncRowIter()


class ORMConnectionHelper:
    """ORM connection helper class"""
    
    def __init__(self, connection):
        self.connection = connection
    
    async def insert_model(self, table_name: str, model):
        """Insert single model object"""
        field_names = model.field_names()
        values = model.to_values()
        
        placeholders = ", ".join(["?" for _ in field_names])
        sql = f"INSERT INTO {table_name} ({', '.join(field_names)}) VALUES ({placeholders})"
        
        return await self.connection.exec(sql, values)
    
    async def query_models(self, model_class, sql: str, params=None):
        """Query and return model object list"""
        rows = await self.connection.query_iter(sql, params or [])
        
        models = []
        async for row in rows:
            model = model_class.from_row(row)
            models.append(model)
        
        return models


# Step definitions

@given('I have databend-driver with ORM support')
def step_given_orm_support(context: Context):
    """Initialize ORM support"""
    context.orm_available = True


@given('I define a simple model without annotations')
def step_define_simple_model(context: Context):
    """Define simple model"""
    context.model_class = SimpleModel
    context.model_instance = SimpleModel(id=1, name="test", active=False)


@given('I define a model with field renaming')
def step_define_renamed_model(context: Context):
    """Define model with field renaming"""
    context.model_class = UserModel
    context.model_instance = UserModel(
        id=1,
        username="alice",
        email="alice@example.com",
        birth_date=date(2000, 1, 1)
    )


@given('I define a model with skip_serializing fields')
def step_define_skip_serializing_model(context: Context):
    """Define model with skip_serializing fields"""
    context.model_class = SkipModel
    context.model_instance = SkipModel(
        id=1,
        name="test",
        created_at=datetime.now(),
        value="should_skip"
    )


@given('I define a model for row mapping')
def step_define_deserialize_model(context: Context):
    """Define model for deserialization testing"""
    context.model_class = DeserializeModel
    context.row_data = [1, "alice", "alice@example.com", date(2000, 1, 1)]


@given('I define a model for error testing')
def step_define_error_model(context: Context):
    """Define model for error testing"""
    context.model_class = SimpleModel
    context.invalid_row_data = "invalid"


@given('I define a model with convenience aliases')
def step_define_alias_model(context: Context):
    """Define model with convenience aliases"""
    context.model_class = AliasModel
    context.model_instance = AliasModel(id=1, name="John Doe")


@given('I define a complex model with mixed annotations')
def step_define_complex_model(context: Context):
    """Define complex model"""
    context.model_class = ComplexModel
    context.model_instance = ComplexModel(
        id=1,
        name="John Doe",
        email="john@example.com",
        updated=datetime.now()
    )


@given('I define a model with renamed fields')
def step_define_model_with_renamed_fields(context: Context):
    """Define model for repr testing"""
    context.model_instance = UserModel(
        id=1,
        username="alice",
        email="alice@example.com",
        birth_date=date(2000, 1, 1)
    )

@given("A new Databend Driver Client")
def _(context):
    dsn = os.getenv(
        "TEST_DATABEND_DSN",
        "databend://root:root@localhost:8000/?sslmode=disable",
    )
    client = databend_driver.BlockingDatabendClient(dsn)
    context.client = client
    context.cursor = client.cursor()
    context.connection_future = client.get_conn()
    context.has_real_db = True



@given('I have an ORM helper instance')
def step_given_orm_helper(context: Context):
    """Set up ORM helper with mock connection"""
    context.mock_connection = MockConnection()
    context.orm_helper = ORMConnectionHelper(context.mock_connection)


@when('I get the field names')
def step_get_field_names(context: Context):
    """Get field names from model class"""
    context.field_names = context.model_class.field_names()


@when('I convert the model to values')
def step_convert_to_values(context: Context):
    """Convert model instance to values"""
    context.values = context.model_instance.to_values()


@when('I create the model from row data')
def step_create_from_row(context: Context):
    """Create model from row data"""
    context.created_model = context.model_class.from_row(context.row_data)


@when('I try to create model from invalid row data')
def step_create_from_invalid_row(context: Context):
    """Try to create model from invalid data"""
    try:
        context.model_class.from_row(context.invalid_row_data)
        context.error_occurred = False
    except ORMError:
        context.error_occurred = True
        context.error_type = ORMError


@when('I check the annotation properties')
def step_check_annotation_properties(context: Context):
    """Check annotation properties"""
    context.rename_field = Rename("alias")
    context.skip_ser_field = SkipSer
    context.skip_deser_field = SkipDeser
    context.skip_both_field = Skip


@when('I perform various operations on the model')
def step_perform_complex_operations(context: Context):
    """Perform various operations on complex model"""
    context.complex_field_names = context.model_instance.field_names()
    context.complex_values = context.model_instance.to_values()
    
    # Test partial row data
    row_data = [1, "John Doe", "john@example.com"]
    context.partial_model = context.model_class.from_row(row_data)


@when('I get the string representation')
def step_get_string_representation(context: Context):
    """Get string representation of model"""
    context.repr_str = repr(context.model_instance)


@when('I create a table and insert ORM models')
def step_create_table_and_insert(context: Context):
    """Create table and insert models (requires real DB)"""
    if not context.has_real_db:
        context.scenario.skip(context.skip_reason)
        return
    
    async def run_db_test():
        conn = await context.connection_future
        try:
            # Create test table
            await conn.exec("""
            CREATE OR REPLACE TABLE test_users (
                id INT NOT NULL,
                user_name STRING NOT NULL,
                email STRING NOT NULL, 
                birth_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                value STRING DEFAULT 'default'
            )
            """)
            
            # Insert test user
            user = UserModel(
                id=1,
                username="alice",
                email="alice@example.com",
                birth_date=date(1990, 1, 15)
            )
            
            field_names = user.field_names()
            values = user.to_values()
            placeholders = ", ".join(["?" for _ in field_names])
            insert_sql = f"INSERT INTO test_users ({', '.join(field_names)}) VALUES ({placeholders})"
            
            rows_affected = await conn.exec(insert_sql, values)
            context.rows_inserted = rows_affected
            
            # Query back
            select_sql = f"SELECT {', '.join(field_names)} FROM test_users WHERE id = ?"
            row = await conn.query_row(select_sql, [1])
            context.retrieved_user = UserModel.from_row(row)
            
        finally:
            await conn.exec("DROP TABLE test_users")
            await conn.close()
    
    asyncio.run(run_db_test())


@when('I use it to insert and query models')
def step_use_orm_helper(context: Context):
    """Use ORM helper to insert and query"""
    async def run_helper_test():
        # Test single insert
        user = UserModel(id=1, username="alice", email="alice@example.com", birth_date=date(1990, 1, 15))
        result = await context.orm_helper.insert_model("users", user)
        context.insert_result = result
        
        # Test query models
        models = await context.orm_helper.query_models(UserModel, "SELECT * FROM users")
        context.queried_models = models
    
    asyncio.run(run_helper_test())


@then('I should get all field names in order')
def step_verify_simple_field_names(context: Context):
    """Verify simple field names"""
    assert context.field_names == ["id", "name", "active"]


@then('I should get all field values in order')
def step_verify_simple_values(context: Context):
    """Verify simple values"""
    assert context.values == [1, "test", False]


@then('I should get renamed field names')
def step_verify_renamed_field_names(context: Context):
    """Verify renamed field names"""
    expected = ["id", "user_name", "email", "birth_date"]
    assert context.field_names == expected


@then('I should get field values in renamed order')
def step_verify_renamed_values(context: Context):
    """Verify values in renamed field order"""
    expected = [1, "alice", "alice@example.com", date(2000, 1, 1)]
    assert context.values == expected


@then('skip_serializing fields should be excluded from field names')
def step_verify_skip_serializing_field_names(context: Context):
    """Verify skip_serializing fields excluded from field names"""
    expected = ["id", "name"]  # created_at and value should be excluded
    assert context.field_names == expected


@then('skip_serializing fields should be excluded from values')
def step_verify_skip_serializing_values(context: Context):
    """Verify skip_serializing fields excluded from values"""
    expected = [1, "test"]  # created_at and value should be excluded
    assert context.values == expected


@then('the model should have correct field values')
def step_verify_model_field_values(context: Context):
    """Verify model has correct field values"""
    model = context.created_model
    assert model.id == 1
    assert model.username == "alice"
    assert model.email == "alice@example.com"
    assert model.birth_date == date(2000, 1, 1)


@then('skip_deserializing fields should use default values')
def step_verify_default_values(context: Context):
    """Verify skip_deserializing fields use defaults"""
    model = context.created_model
    assert model.created_at is None  # skip_serializing but not skip_deserializing
    assert model.value == "default"  # skip_serializing field
    assert model.internal_field == ""  # skip_both field


@then('I should get an ORMError')
def step_verify_orm_error(context: Context):
    """Verify ORMError was raised"""
    assert context.error_occurred is True
    assert context.error_type == ORMError


@then('the aliases should work the same as full functions')
def step_verify_aliases(context: Context):
    """Verify convenience aliases work correctly"""
    assert context.rename_field.rename == "alias"
    assert context.skip_ser_field.skip_serializing is True
    assert context.skip_deser_field.skip_deserializing is True
    assert context.skip_both_field.skip_serializing is True
    assert context.skip_both_field.skip_deserializing is True


@then('all annotation behaviors should work correctly')
def step_verify_complex_behaviors(context: Context):
    """Verify complex model behaviors"""
    # Field names should include renaming and exclude skip_serializing
    expected_names = ["id", "full_name", "email", "updated"]
    assert context.complex_field_names == expected_names
    
    # Values should exclude skip_serializing fields
    assert len(context.complex_values) == 4  # id, name, email, updated
    assert context.complex_values[0] == 1
    assert context.complex_values[1] == "John Doe"
    assert context.complex_values[2] == "john@example.com"
    
    # Partial model should handle missing fields correctly
    model = context.partial_model
    assert model.id == 1
    assert model.name == "John Doe"
    assert model.email == "john@example.com"
    assert model.created is None  # skip_serializing but not skip_deserializing
    assert model.updated is None  # skip_deserializing, use default value
    assert model.secret == "secret"  # skip_both, use default value


@then('it should show renamed fields clearly')
def step_verify_repr_shows_renamed_fields(context: Context):
    """Verify repr shows renamed fields"""
    assert "username(user_name)='alice'" in context.repr_str
    assert "UserModel(" in context.repr_str


@then('I should be able to query them back as models')
def step_verify_real_db_operations(context: Context):
    """Verify real database operations worked"""
    if not context.has_real_db:
        return  # Test was skipped
    
    assert context.rows_inserted == 1
    retrieved = context.retrieved_user
    assert retrieved.id == 1
    assert retrieved.username == "alice"
    assert retrieved.email == "alice@example.com"
    assert retrieved.birth_date == date(1990, 1, 15)


@then('it should handle the database operations correctly')
def step_verify_orm_helper_operations(context: Context):
    """Verify ORM helper operations"""
    assert context.insert_result == 1
    assert len(context.mock_connection.executed_sqls) == 1
    assert "INSERT INTO users" in context.mock_connection.executed_sqls[0]
    assert context.mock_connection.executed_params[0] == [1, "alice", "alice@example.com", date(1990, 1, 15)]
    
    assert len(context.queried_models) == 2
    assert context.queried_models[0].username == "alice"
    assert context.queried_models[1].username == "bob"