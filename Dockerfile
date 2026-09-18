FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /service ./cmd/service
FROM alpine:3.22
COPY --from=build /service /service
COPY contracts/recovery-policy.json /etc/station-telemetry/recovery-policy.json
ENV CONTRACT_PATH=/etc/station-telemetry/recovery-policy.json
EXPOSE 8080
CMD ["/service"]
