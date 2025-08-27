FROM alpine

ARG TARGETPLATFORM

COPY dist/${TARGETPLATFORM}/ttc-server /usr/bin/ttc-server

RUN chmod +x /usr/bin/ttc-server

# docker run --net host datafuselabs/ttc-rust -P 9092 --databend_dsn databend://default:@127.0.0.1:8000
CMD ["ttc-server"]
