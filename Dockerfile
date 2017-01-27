FROM golang:1.8-alpine

RUN apk --update add git #&& \
    # go get \
    #   github.com/golang/dep

RUN mkdir -p /go/src/github.com/minodisk/go-learn-dbr
WORKDIR /go/src/github.com/minodisk/go-learn-dbr
COPY . .

CMD ls -al vendor/github.com/go-sql-driver/mysql && go run main.go
