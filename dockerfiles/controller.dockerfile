FROM golang:1.18.1-buster AS build
WORKDIR code
ENV CGO_ENABLED=0
ENV DEBIAN_FRONTEND=noninteractive
COPY find_providers .
RUN rm go.sum
RUN go mod download && go mod tidy
RUN go build -o /out/controller controller.go

FROM debian:buster-slim as app

COPY --from=build /out/controller /

ENTRYPOINT ["./controller"]


