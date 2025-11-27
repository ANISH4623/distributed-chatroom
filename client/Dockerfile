# Build stage
FROM golang:1.24-alpine AS builder

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the client
RUN CGO_ENABLED=0 GOOS=linux go build -o /client ./client

# Run stage
FROM alpine:3.19

WORKDIR /

COPY --from=builder /client /client

ENTRYPOINT ["/client"]
