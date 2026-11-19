# syntax=docker/dockerfile:1
# Build from the repository root: docker build -f services/satellite-ingestor/Dockerfile .
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY services/satellite-ingestor/go.mod services/satellite-ingestor/go.sum ./
COPY contracts /contracts
COPY observability /observability
RUN go mod download
COPY services/satellite-ingestor/. .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/service ./cmd/satellite-ingestor

COPY scripts/healthcheck /build/healthcheck
WORKDIR /build/healthcheck
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/healthcheck .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/service /service
COPY --from=builder /bin/healthcheck /healthcheck
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/service"]
