FROM golang:1.23-alpine

RUN apk add --no-cache \
    bash \
    git

ADD main.go go.mod index.html /app/

WORKDIR /app
RUN go get anomalies
RUN go build
RUN rm main.go go.mod index.html

EXPOSE 9090

ENTRYPOINT ["/app/anomalies"]