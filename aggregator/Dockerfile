FROM golang:alpine AS builder

WORKDIR /build

ADD go.mod .

COPY . .

RUN go build -ldflags="-s -w" -o /app/aggregator ./cmd

FROM alpine

WORKDIR /app

COPY --from=builder /app/aggregator /app/aggregator

CMD ["./aggregator"]