FROM alpine

ARG TARGETPLATFORM

COPY dist/${TARGETPLATFORM}/ttc-server /usr/bin/ttc-server

RUN chmod +x /usr/bin/ttc-server

CMD ["ttc-server"]
