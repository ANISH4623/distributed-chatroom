# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy all source code and vendored dependencies
COPY . .

# Build the server with vendored dependencies
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /server ./server

# Run stage
FROM alpine:3.19

WORKDIR /

COPY --from=builder /server /server

EXPOSE 8080 9090

ENTRYPOINT ["/server"]
