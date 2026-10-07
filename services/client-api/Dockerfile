# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY contracts ./contracts
WORKDIR /app/services/client-api
COPY services/client-api/go.mod services/client-api/go.sum ./
RUN go mod download
COPY services/client-api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/service ./cmd/main

COPY scripts/healthcheck /build/healthcheck
WORKDIR /build/healthcheck
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/healthcheck .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/service /service
COPY --from=builder /bin/healthcheck /healthcheck
USER nonroot:nonroot
ENTRYPOINT ["/service"]
