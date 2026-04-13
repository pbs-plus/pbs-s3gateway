# Run stage - binary is pre-built by GoReleaser
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY pbs-s3gateway /usr/local/bin/pbs-s3gateway
EXPOSE 8080
ENTRYPOINT ["pbs-s3gateway"]
