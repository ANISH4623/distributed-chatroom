# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the client
RUN CGO_ENABLED=0 GOOS=linux go build -o /client ./client

# Run stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /

COPY --from=builder /client /client

ENTRYPOINT ["/client"]
