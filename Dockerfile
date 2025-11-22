FROM golang:1.23-alpine

RUN apk add --no-cache \
    bash \
    git

COPY main.go go.mod index.html style.css /app/
COPY dist /app/dist

WORKDIR /app
RUN go get outliers
RUN go build
RUN rm -r main.go go.mod index.html style.css ./dist 

EXPOSE 9090

ENTRYPOINT ["/app/outliers"]