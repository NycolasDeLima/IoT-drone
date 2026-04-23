FROM golang:1.25.1

WORKDIR /app

COPY go.mod .
RUN go mod download

COPY . .
RUN go build -o app

CMD ["./app"]