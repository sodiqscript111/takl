FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X takl/internal/version.Version=${VERSION:-dev}" -o /takld ./cmd/takld
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /taklctl ./cmd/taklctl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /takld /usr/local/bin/
COPY --from=builder /taklctl /usr/local/bin/
EXPOSE 8090 8100 7946/tcp 7946/udp
ENTRYPOINT ["takld"]
