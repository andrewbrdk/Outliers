package main

import (
	"embed"
	"log"
	"net/http"
	"os"
)

//go:embed index.html
var embedded embed.FS

var infoLog *log.Logger
var errorLog *log.Logger

var FC Forecasts
var CONF Config

type Forecasts struct {
	Forecasts       map[int]*Forecast
	forecastCounter int
}

type Config struct {
	port string
}

type Forecast struct {
	Title string `toml:"title"`
}

func main() {
	initConfig()
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	httpServer()
}

func initConfig() {
	CONF.port = ":9090"
}

func httpServer() {
	http.HandleFunc("/", httpIndex)
	log.Fatal(http.ListenAndServe(CONF.port, nil))
}

func httpIndex(w http.ResponseWriter, r *http.Request) {
	data, err := embedded.ReadFile("index.html")
	if err != nil {
		http.Error(w, "Error loading the page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write(data)
}
