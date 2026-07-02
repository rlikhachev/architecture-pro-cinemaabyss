FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY main.go ./

ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a -installsuffix cgo -o proxy-service main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/proxy-service .

EXPOSE 8000

CMD ["./proxy-service"]
