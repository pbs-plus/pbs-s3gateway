# Build stage
FROM golang:1.26-alpine AS builder
WORKDIR /app

# Copy everything including the local pxar-local copy
COPY . .

# go.mod already has the replace ./pxar-local
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o pbs-s3gateway ./cmd/pbs-s3gateway

# Run stage
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/pbs-s3gateway /usr/local/bin/pbs-s3gateway
EXPOSE 8080
ENTRYPOINT ["pbs-s3gateway"]
