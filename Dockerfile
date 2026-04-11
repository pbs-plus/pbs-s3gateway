FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /pbs-s3gateway ./cmd/pbs-s3gateway

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /pbs-s3gateway /usr/local/bin/pbs-s3gateway
EXPOSE 8080
ENTRYPOINT ["pbs-s3gateway"]
