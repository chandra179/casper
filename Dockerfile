# Build stage
FROM golang:1.26.3-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/main .

# Run stage - using Alpine for very small image
FROM alpine:3.21

RUN apk add --no-cache ca-certificates
COPY --from=builder /app/main /app/main

ENTRYPOINT ["/app/main"]