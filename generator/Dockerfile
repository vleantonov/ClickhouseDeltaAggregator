FROM golang:alpine AS builder

WORKDIR /build

ADD go.mod .

COPY . .

RUN go build -ldflags="-s -w" -o /app/generator ./cmd

FROM alpine

WORKDIR /app

COPY --from=builder /app/generator /app/generator

CMD ["./generator"]