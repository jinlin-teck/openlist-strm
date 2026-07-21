# ---- 构建阶段 ----
FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/openlist-strm .

# ---- 运行阶段 ----
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/openlist-strm /app/openlist-strm
COPY config.example.yaml /app/config.example.yaml

EXPOSE 8080
VOLUME ["/app/config"]
ENTRYPOINT ["/app/openlist-strm"]
CMD ["--config", "/app/config/config.yaml"]
