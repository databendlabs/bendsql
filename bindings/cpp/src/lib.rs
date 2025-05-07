use databend_driver::{Client as DatabendClient, Connection as DatabendConnection};
use cxx::CxxString;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        type DatabendClientWrapper;
        type DatabendConnectionWrapper;

        fn new_client(dsn: &CxxString) -> Box<DatabendClientWrapper>;
        fn get_connection(client: &DatabendClientWrapper) -> Box<DatabendConnectionWrapper>;
        fn execute_query(connection: &DatabendConnectionWrapper, query: &CxxString) -> bool;
        fn get_version(client: &DatabendClientWrapper) -> String;
        fn query_row(connection: &DatabendConnectionWrapper, query: &CxxString) -> String;
    }
}

pub struct DatabendClientWrapper {
    client: DatabendClient,
}

pub struct DatabendConnectionWrapper {
    connection: DatabendConnection,
}

impl DatabendClientWrapper {
    fn new(dsn: &CxxString) -> Box<Self> {
        Box::new(Self {
            client: DatabendClient::new(dsn.to_str().unwrap()),
        })
    }

    fn get_connection(&self) -> Box<DatabendConnectionWrapper> {
        let connection = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.client.get_conn())
            .unwrap();
        Box::new(DatabendConnectionWrapper { connection })
    }

    fn get_version(&self) -> String {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.client.version())
            .unwrap_or_else(|_| "unknown".to_string())
    }
}

impl DatabendConnectionWrapper {
    fn execute_query(&self, query: &CxxString) -> bool {
        let query_str = query.to_str().unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.connection.exec(query_str, None))
            .is_ok()
    }

    fn query_row(&self, query: &CxxString) -> String {
        let query_str = query.to_str().unwrap();
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.connection.query_row(query_str, None));

        match result {
            Ok(Some(row)) => format!("{:?}", row.values()),
            Ok(None) => "No rows returned".to_string(),
            Err(err) => format!("Error: {}", err),
        }
    }
}
