FROM golang:1.18.1-buster AS build
WORKDIR code
ENV CGO_ENABLED=0
ENV DEBIAN_FRONTEND=noninteractive
COPY find_providers .
RUN go mod download
RUN go build -o /out/writer write_find_provider_logs_to_db.go

FROM debian:buster-slim as app

COPY --from=build /out/writer /

ENTRYPOINT ["./writer"]


