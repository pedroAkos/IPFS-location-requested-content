FROM golang:1.18.1-buster AS build
WORKDIR code
ENV CGO_ENABLED=0
COPY find_providers .
RUN go mod download
RUN go build -o /out/test_ipfs_connection test_ipfs_connection.go

FROM debian:buster-slim as app

#RUN sysctl -w net.core.rmem_max=2500000
COPY --from=build /out/test_ipfs_connection /

ENTRYPOINT ["./test_ipfs_connection"]


