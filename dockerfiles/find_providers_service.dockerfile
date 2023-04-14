FROM golang:1.18.1-buster AS build
WORKDIR code
ENV CGO_ENABLED=0
COPY find_providers .
RUN rm go.sum
RUN go mod download && go mod tidy
RUN go build -o /out/find_providers find_providers_service.go

FROM debian:buster-slim as app

#RUN sysctl -w net.core.rmem_max=2500000
COPY --from=build /out/find_providers /

ENTRYPOINT ["./find_providers"]


