# Dockerfile
FROM rust as builder

# Set the current working directory inside the container
WORKDIR /usr/src

# Copy the source code into the container
COPY . .

# Build the application
RUN cargo build --bin ttc-server --package databend-ttc --release

RUN cp /usr/src/target/release/ttc-server /usr/local/bin/ttc-server

# Set the startup command
# docker run --net host datafuselabs/ttc-rust -P 9901 --databend_dsn databend://default:@127.0.0.1:8000
CMD ["ttc-server"]
