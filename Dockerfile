FROM golang:alpine AS builder

WORKDIR /app

COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o myApp main.go

FROM alpine

WORKDIR /app

COPY --from=builder /app/myApp ./myApp
COPY index.html ./index.html

CMD ["./myApp"]