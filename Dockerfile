# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git for go mod download
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binaries
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o aegis ./cmd/aegis
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o aegis-test ./cmd/aegis-test

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/aegis .
COPY --from=builder /app/aegis-test .

# Create data directory
RUN mkdir -p /data/wal

# Expose ports
# 7700 - Client port
# 7701 - Gossip port
EXPOSE 7700 7701

# Health check
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 7700 || exit 1

# Default entrypoint
ENTRYPOINT ["/app/aegis"]

# Default arguments
CMD ["--bind=0.0.0.0:7700", "--gossip=0.0.0.0:7701", "--data-dir=/data"]
