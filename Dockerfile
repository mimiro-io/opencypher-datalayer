FROM golang:1.23-alpine AS build_env

# Copy go mod and sum files
COPY go.mod go.sum ./

# Install SSL ca certificates.
RUN apk update && apk add --no-cache \
    ca-certificates \
    tzdata && \
    update-ca-certificates

# Download dependencies
RUN go mod download

FROM build_env AS builder
COPY layer.go neo4j.go ./
COPY cmd ./cmd

# Build the app binaries
RUN go build -a -o /opencypher-datalayer ./cmd/main.go

FROM alpine:3.21

RUN apk update && apk upgrade --no-cache

# Copy binaries and certificates
COPY --from=builder /opencypher-datalayer /opencypher-datalayer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Set entrypoint
CMD ["./opencypher-datalayer"]