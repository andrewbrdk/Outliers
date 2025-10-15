package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	Id               int    `toml:"-"`
	ConnectionString string `toml:"connection"`
	DataTable        string `toml:"source"`
	OutputTable      string `toml:"output"`
	ForecastHorizon  int    `toml:"forecast_horizon"`
	ForecastModel    string `toml:"forecast_model"`
}

type Point struct {
	t     int64
	value float64
}

func main() {
	initConfig()
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	FC.loadForecasts(CONF.confFile)
	// todo: run forecasts on schedule
	for _, fc := range FC.Forecasts {
		err := doForecast(fc)
		if err != nil {
			errorLog.Printf("Error doing forecast '%s': %v", fc.Title, err)
		}
	}
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

func (FC *Forecasts) loadForecasts(filename string) error {
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
		f.Id = FC.forecastCounter
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

func doForecast(fc *Forecast) error {
	infoLog.Printf("Forecasting %s", fc.Title)
	data, err := readTimeSeries(fc)
	forecast, err := trainAndPredict(fc, data)
	if err != nil {
		return err
	}
	err = writeForecast(fc, forecast)
	if err != nil {
		return err
	}
	return nil
}

func readTimeSeries(fc *Forecast) ([]Point, error) {
	selectQuery := fmt.Sprintf(`
		select 
			dt - MIN(dt) OVER () AS ts, 
			views as value 
		from %s
		order by ts asc
	`, fc.DataTable)
	ctx := context.Background()
	db, err := sql.Open("pgx", fc.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, selectQuery)
	if err != nil {
		errorLog.Printf("Error reading from table: %v", err)
	}
	defer rows.Close()

	var data []Point
	for rows.Next() {
		var p Point
		if err := rows.Scan(&p.t, &p.value); err != nil {
			errorLog.Printf("Scan error: %v", err)
		}
		data = append(data, p)
	}
	if err := rows.Err(); err != nil {
		errorLog.Printf("Rows error: %v", err)
	}
	return data, nil
}

func trainAndPredict(fc *Forecast, data []Point) ([]Point, error) {
	infoLog.Printf("Training model %s for forecast horizon %d", fc.ForecastModel, fc.ForecastHorizon)
	if len(data) == 0 {
		errorLog.Printf("No data points to forecast")
		return nil, errors.New("No data points to forecast")
	}
	if fc.ForecastModel != "naive" {
		errorLog.Printf("Unsupported model: %s", fc.ForecastModel)
		return nil, errors.New("Unsupported model: " + fc.ForecastModel)
	}
	lastPoint := data[len(data)-1]
	var forecast []Point
	for i := 1; i <= fc.ForecastHorizon; i++ {
		fcPoint := Point{
			t:     lastPoint.t + int64(i),
			value: lastPoint.value,
		}
		forecast = append(forecast, fcPoint)
	}
	infoLog.Printf("Forecast complete, generated %d points", len(forecast))
	return forecast, nil
}

func writeForecast(fc *Forecast, forecast []Point) error {
	destTable := fc.OutputTable
	ctx := context.Background()
	db, err := sql.Open("pgx", fc.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
	}
	defer db.Close()

	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			dt DATE,
			step int,
			value numeric
		)`, destTable)
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		errorLog.Printf("Error creating table %s: %v", destTable, err)
		return err
	}

	for _, p := range forecast {
		upsertQuery := fmt.Sprintf(`
			INSERT INTO %s (dt, step, value) VALUES (NULL, $1, $2)
		`, destTable)

		if _, err := db.ExecContext(ctx, upsertQuery, p.t, p.value); err != nil {
			errorLog.Printf("Error inserting forecast for %d: %v", p.t, err)
			return err
		}
	}

	return nil
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func httpServer() {
	http.HandleFunc("/", httpIndex)
	http.HandleFunc("/forecasts", httpForecasts)
	http.HandleFunc("/api/forecast/update", forecastUpdateHandler)
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

func forecastUpdateHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: "missing id"})
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: "invalid id"})
		return
	}
	fc := FC.Forecasts[id]
	if fc == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: "forecast not found"})
		return
	}
	if err := doForecast(fc); err != nil {
		log.Println("Forecast update error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(Response{Status: "ok"})
}
