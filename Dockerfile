FROM golang:alpine AS builder

WORKDIR /app

COPY . .

RUN go mod install
RUN go build -o myApp .

FROM alpine

WORKDIR /app

COPY --from=builder /app/myApp /app/myApp

CMD ["./myApp"]