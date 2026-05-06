Feature: Databend Driver Local Mode

    Scenario: Local connect with persistent path
        Given Real local embedded dependencies are available
        When A new local embedded connection is created
        Then Local select 1 should equal 1

    Scenario: Local connect with memory target
        Given Real local embedded dependencies are available
        When A new local memory connection is created
        Then Local numbers aggregate should match expected values

    Scenario: Local explicit memory dsn parsing
        Given Real local embedded dependencies are available
        Then Local explicit memory dsn should parse as memory mode

    Scenario: Local execute and query roundtrip
        Given Real local embedded dependencies are available
        When A new local embedded connection is created
        Then Local execute should create and populate a table

    Scenario: Local tenant mode
        Given Real local embedded dependencies are available
        When A new local tenant connection is created
        Then Local tenant connection should use the configured data path

    Scenario: Local register parquet
        Given Real local embedded dependencies are available
        When A parquet file is registered in local mode
        Then Local parquet query should return expected rows

    Scenario: Local dsn connect
        Given Real local embedded dependencies are available
        When A new local dsn connection is created
        Then Local dsn connection should execute queries

    Scenario: Local tenant dsn connect
        Given Real local embedded dependencies are available
        When A new local tenant dsn connection is created
        Then Local tenant dsn connection should execute queries

    Scenario: Local import error message
        Then Local import error should mention Python 3.12 requirement

    Scenario: Local blocking query api
        Given Real local embedded dependencies are available
        When A new local embedded connection is created
        Then Local blocking query api should behave like expected

    Scenario: Local parameter formatting
        Given Real local embedded dependencies are available
        When A new local embedded connection is created
        Then Local parameter formatting should behave like expected
