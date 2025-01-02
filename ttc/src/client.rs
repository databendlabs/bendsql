use std::io::Write;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:9901").await?;

    loop {
        // Prepare a sql
        let mut sql = String::new();
        print!("> ");
        std::io::stdout().flush().unwrap(); // Make sure the prompt is immediately displayed
        std::io::stdin().read_line(&mut sql).unwrap();

        // If the sql is "exit", break the loop
        if sql.trim() == "exit" || sql.trim() == "quit" {
            break;
        }

        let len = sql.len() as u32;
        let len_bytes = len.to_be_bytes();

        // Create a buffer with the length of the sql and the sql itself
        let mut buffer = Vec::with_capacity(4 + sql.len());
        buffer.extend_from_slice(&len_bytes);
        buffer.extend_from_slice(sql.as_bytes());

        // Send the sql
        stream.write_all(&buffer).await?;

        let mut len_bytes = [0; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Read the response
        let mut response = vec![0; len];
        stream.read_exact(&mut response).await?;

        let response: Response = serde_json::from_reader(response.as_slice()).unwrap();
        // Print the response
        println!("response: {:?}", response);
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Response {
    values: Vec<Vec<Option<String>>>,
    error: Option<String>,
}
