# Dockerfile

# Use the official Rust image as the base image
FROM rust as builder

# Set the current working directory inside the container
WORKDIR /usr/src

# Copy the source code into the container
COPY . .

# Build the application
RUN cargo build --bin ttc-server --package databend-ttc --release

# Start a new stage with a minimal image
FROM debian:buster-slim

# Copy the binary from the builder stage
COPY --from=builder /usr/src/target/release/ttc-server /usr/local/bin/ttc-server

# Set the startup command
# docker run datafuselabs/ttc-rust -P 9901 --databend_dsn databend://default:@127.0.0.1:8000
CMD ["ttc-server"]
