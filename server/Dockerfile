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

# Build the server
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ./server

# Run stage
FROM alpine:3.19

WORKDIR /

COPY --from=builder /server /server

EXPOSE 8080 9090

ENTRYPOINT ["/server"]
