FROM golang:1.18.1-buster AS build
WORKDIR code
ENV CGO_ENABLED=0
COPY find_providers .
RUN go mod download
RUN go build -o /out/find_providers find_providers_service.go

FROM debian:buster-slim as app

#RUN sysctl -w net.core.rmem_max=2500000
COPY --from=build /out/find_providers /

ENTRYPOINT ["./find_providers"]


