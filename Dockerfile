# ---- Build stage ----
FROM golang:1.24.6-alpine AS builder

# Install git and build tools (needed for go mod)
RUN apk add --no-cache git

ENV GOPROXY=https://goproxy.cn,direct


# Set working directory
WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the Go binary statically
RUN go build -o server .

# ---- Run stage ----
FROM alpine:3.20

# Install certificates (for HTTPS calls to Pixie Cloud)
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/server .
# Copy config file (adjust if you mount it instead)
COPY config.json .

# Run server on port 8080
EXPOSE 8080
CMD ["./server"]
