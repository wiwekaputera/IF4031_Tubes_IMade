# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY services/bmkg-mock/go.mod services/bmkg-mock/go.sum ./
COPY contracts /contracts
RUN go mod download
COPY services/bmkg-mock/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./cmd/main

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/service /service
ENTRYPOINT ["/service"]