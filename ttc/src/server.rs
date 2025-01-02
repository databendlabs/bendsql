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

use bytes::BytesMut;
use databend_driver::{Client, Connection, Row, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use bytes::Buf;
use clap::{command, Parser};

#[derive(Debug, Clone, Parser, PartialEq)]
#[command(name = "ttc")]
struct Config {
    #[clap(short = 'P', default_value = "9901", env = "TTC_PORT", long)]
    port: u16,
    #[clap(
        long,
        env = "DATABEND_DSN",
        hide_env_values = true,
        default_value = "databend://default:@127.0.0.1:8000"
    )]
    databend_dsn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Response {
    values: Vec<Vec<Option<String>>>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();

    // check dsn
    {
        println!(
            "Start to check databend dsn: {} is valid",
            config.databend_dsn
        );
        let client = Client::new(config.databend_dsn.clone());
        let conn = client.get_conn().await.unwrap();
        println!("Databend version: {}", conn.version().await.unwrap());
    }

    let l = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&l).await?;
    println!("Rust TTC Server running on {l}");

    loop {
        let (socket, _) = listener.accept().await?;
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(e) = process(socket, &config).await {
                eprintln!("Error processing connection: {:?}", e);
            }
        });
    }
}

async fn process(mut socket: TcpStream, config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::with_capacity(1024);
    // Initialize a Client and get a connection
    let client = Client::new(config.databend_dsn.clone());
    let mut conn = client.get_conn().await?;

    loop {
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        while let Some((frame, size)) = decode_frame(&buf) {
            execute_command(&frame, &mut socket, conn.as_mut()).await?;
            buf.advance(size);
        }
    }
}

fn decode_frame(buf: &BytesMut) -> Option<(Vec<u8>, usize)> {
    // We need at least 4 bytes to read the length
    if buf.len() < 4 {
        return None;
    }

    // Read the first 4 bytes as a u32 to get the length of the message
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // Check if the buffer contains the full message
    if buf.len() < 4 + len {
        return None;
    }

    // Extract the message
    let message = buf[4..4 + len].to_vec();

    // Return the message and the total length of the frame (4 bytes for length + message length)
    Some((message, 4 + len))
}

async fn execute_command(
    command: &[u8],
    socket: &mut TcpStream,
    conn: &mut dyn Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    let command_str = String::from_utf8_lossy(command);

    let results = conn.query_all(&command_str).await;

    let mut response = Response {
        values: vec![],
        error: None,
    };
    match results {
        Ok(results) => {
            response.values = results.into_iter().map(|row| row_to_vec(row)).collect();
        }
        Err(err) => response.error = Some(err.to_string()),
    }

    let response = serde_json::to_vec(&response).unwrap();

    // Calculate the length of the command and convert it to bytes
    let len = response.len() as u32;
    let len_bytes = len.to_be_bytes();
    // Create a buffer with the length of the command and the command itself
    let mut buffer = Vec::with_capacity(4 + response.len());
    buffer.extend_from_slice(&len_bytes);
    buffer.extend_from_slice(&response);

    // Send the buffer to the client
    socket.write_all(&buffer).await?;

    Ok(())
}

fn row_to_vec(row: Row) -> Vec<Option<String>> {
    row.into_iter()
        .map(|v| {
            if v == Value::Null {
                None
            } else {
                Some(v.to_string())
            }
        })
        .collect()
}
