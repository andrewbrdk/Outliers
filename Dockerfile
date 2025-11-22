FROM golang:1.23-alpine

RUN apk add --no-cache \
    bash \
    git \
    wget

COPY main.go go.mod index.html style.css /app/
RUN wget -P /app/dist \
    https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/uPlot.iife.min.js \
    https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/uPlot.min.css \
    https://cdn.jsdelivr.net/npm/flatpickr@4.6.13/dist/flatpickr.min.js \
    https://cdn.jsdelivr.net/npm/flatpickr@4.6.13/dist/flatpickr.min.css

WORKDIR /app
RUN go get outliers
RUN go build
RUN rm -r main.go go.mod index.html style.css ./dist 

EXPOSE 9090

ENTRYPOINT ["/app/outliers"]