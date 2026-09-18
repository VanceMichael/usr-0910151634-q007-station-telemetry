FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
# 依赖已随 vendor/ 一起提交，构建无需联网；无数据库时集成用例自动跳过。
RUN CGO_ENABLED=0 go test -mod=vendor ./... && CGO_ENABLED=0 go build -mod=vendor -o /service ./cmd/service

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget
COPY --from=build /service /service
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=10 \
  CMD wget -qO- http://127.0.0.1:8080/health || exit 1
CMD ["/service"]
