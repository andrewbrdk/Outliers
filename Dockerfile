FROM golang:1.23-alpine

RUN apk add --no-cache \
    bash \
    git

ADD main.go go.mod index.html style.css ./libs /app/

WORKDIR /app
RUN go get outliers
RUN go build
RUN rm -r main.go go.mod index.html style.css ./libs 

EXPOSE 9090

ENTRYPOINT ["/app/outliers"]