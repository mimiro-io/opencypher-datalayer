FROM golang:1.24-alpine AS build

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

COPY layer.go neo4j.go ./
COPY cmd ./cmd

# Build the app binaries
RUN go build -a -o /opencypher-datalayer ./cmd/main.go

FROM gcr.io/distroless/static-debian12:nonroot

# Copy binaries
COPY --from=build /opencypher-datalayer /opencypher-datalayer

# Set entrypoint
CMD ["/opencypher-datalayer"]