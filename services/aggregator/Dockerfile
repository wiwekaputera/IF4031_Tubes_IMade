# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY services/aggregator/go.mod services/aggregator/go.sum ./
COPY contracts /contracts
RUN go mod download
COPY services/aggregator/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./cmd/main

COPY scripts/healthcheck /build/healthcheck
WORKDIR /build/healthcheck
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/healthcheck .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/service /service
COPY --from=builder /bin/healthcheck /healthcheck
ENTRYPOINT ["/service"]

