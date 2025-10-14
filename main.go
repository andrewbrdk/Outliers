package main

import (
	"embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/BurntSushi/toml"
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
	ForecastsToml   []*Forecast `toml:"forecasts"`
}

type Config struct {
	port     string
	confFile string
}

type Forecast struct {
	Title            string `toml:"title"`
	ConnectionString string `toml:"connection"`
	DataTable        string `toml:"source"`
	OutputTable      string `toml:"output"`
}

func main() {
	initConfig()
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	loadForecasts(CONF.confFile)
	httpServer()
}

func initConfig() {
	CONF.port = ":9090"
	CONF.confFile = "forecasts.toml"
	if port := os.Getenv("FCAST_PORT"); port != "" {
		CONF.port = ":" + port
	}
	if confFile := os.Getenv("FCAST_CONF_FILE"); confFile != "" {
		CONF.confFile = confFile
	}
}

func loadForecasts(filename string) error {
	f, err := os.ReadFile(filename)
	if err != nil {
		errorLog.Printf("Error reading file %s: %v\n", filename, err)
		return err
	}
	err = toml.Unmarshal(f, &FC)
	if err != nil {
		errorLog.Printf("Error parsing file %s: %v\n", filename, err)
		return err
	}
	FC.Forecasts = make(map[int]*Forecast)
	for i, f := range FC.ForecastsToml {
		err := validateForecast(f)
		if err != nil {
			errorLog.Printf("Skipping invalid forecast at index %d", i)
			continue
		}
		FC.Forecasts[FC.forecastCounter] = f
		FC.forecastCounter += 1
	}
	infoLog.Printf("Loaded %d forecasts from %s", len(FC.Forecasts), filename)
	return nil
}

func validateForecast(f *Forecast) error {
	if f.Title == "" {
		errorLog.Printf("Forecast missing title")
		return errors.New("Invalid Forecast: missing title")
	}
	if f.ConnectionString == "" {
		errorLog.Printf("Forecast '%s' missing connection string", f.Title)
		return errors.New("Invalid Forecast: missing connection string")
	}
	if f.DataTable == "" {
		errorLog.Printf("Forecast '%s' missing source table", f.Title)
		return errors.New("Invalid Forecast: missing source table")
	}
	if f.OutputTable == "" {
		errorLog.Printf("Forecast '%s' missing output table", f.Title)
		return errors.New("Invalid Forecast: missing output table")
	}
	return nil
}

func httpServer() {
	http.HandleFunc("/", httpIndex)
	http.HandleFunc("/forecasts", httpForecasts)
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

func httpForecasts(w http.ResponseWriter, r *http.Request) {
	fData, err := json.Marshal(FC)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "No Forecasts Found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(fData)
}
