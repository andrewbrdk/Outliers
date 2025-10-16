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

var AD AnomaliesDetectors
var CONF Config

type AnomaliesDetectors struct {
	ADetectors map[int]*ADetector
	counter    int
	Parsed     []*ADetector `toml:"timeseries"`
}

type Config struct {
	port     string
	confFile string
}

type ADetector struct {
	Title            string `toml:"title"`
	Id               int    `toml:"-"`
	ConnectionString string `toml:"connection"`
	DataTable        string `toml:"source"`
	OutputTable      string `toml:"output"`
	ForecastHorizon  int    `toml:"forecast_horizon"`
	ForecastModel    string `toml:"forecast_model"`
}

type Point struct {
	T     int64
	Value float64
}

func main() {
	initConfig()
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	AD.loadDetectors(CONF.confFile)
	// todo: run forecasts on schedule
	for _, ad := range AD.ADetectors {
		err := doForecast(ad)
		if err != nil {
			errorLog.Printf("Error doing forecast '%s': %v", ad.Title, err)
		}
	}
	httpServer()
}

func initConfig() {
	CONF.port = ":9090"
	CONF.confFile = "anomalies.toml"
	if port := os.Getenv("ANOMALIES_PORT"); port != "" {
		CONF.port = ":" + port
	}
	if confFile := os.Getenv("ANOMALIES_CONF_FILE"); confFile != "" {
		CONF.confFile = confFile
	}
}

func (AD *AnomaliesDetectors) loadDetectors(filename string) error {
	f, err := os.ReadFile(filename)
	if err != nil {
		errorLog.Printf("Error reading file %s: %v\n", filename, err)
		return err
	}
	err = toml.Unmarshal(f, AD)
	if err != nil {
		errorLog.Printf("Error parsing file %s: %v\n", filename, err)
		return err
	}
	AD.ADetectors = make(map[int]*ADetector)
	for i, ad := range AD.Parsed {
		err := validateConf(ad)
		if err != nil {
			errorLog.Printf("Skipping invalid config at index %d", i)
			continue
		}
		AD.ADetectors[AD.counter] = ad
		ad.Id = AD.counter
		AD.counter += 1
	}
	infoLog.Printf("Loaded %d configs from %s", len(AD.ADetectors), filename)
	return nil
}

func validateConf(ad *ADetector) error {
	if ad.Title == "" {
		errorLog.Printf("Config missing title")
		return errors.New("Invalid Config: missing title")
	}
	if ad.ConnectionString == "" {
		errorLog.Printf("Config '%s' missing connection string", ad.Title)
		return errors.New("Invalid Config: missing connection string")
	}
	if ad.DataTable == "" {
		errorLog.Printf("Config '%s' missing source table", ad.Title)
		return errors.New("Invalid Config: missing source table")
	}
	if ad.OutputTable == "" {
		errorLog.Printf("Config '%s' missing output table", ad.Title)
		return errors.New("Invalid Config: missing output table")
	}
	return nil
}

func doForecast(ad *ADetector) error {
	infoLog.Printf("Forecasting %s", ad.Title)
	data, err := readTimeSeries(ad)
	forecast, err := trainAndPredict(ad, data)
	if err != nil {
		return err
	}
	err = writeForecast(ad, forecast)
	if err != nil {
		return err
	}
	return nil
}

func readTimeSeries(ad *ADetector) ([]Point, error) {
	selectQuery := fmt.Sprintf(`
		select 
			dt - MIN(dt) OVER () AS ts, 
			views as value 
		from %s
		order by ts asc
	`, ad.DataTable)
	ctx := context.Background()
	db, err := sql.Open("pgx", ad.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, selectQuery)
	if err != nil {
		errorLog.Printf("Error reading from table: %v", err)
		return nil, err
	}
	defer rows.Close()

	var data []Point
	for rows.Next() {
		var p Point
		if err := rows.Scan(&p.T, &p.Value); err != nil {
			errorLog.Printf("Scan error: %v", err)
		}
		data = append(data, p)
	}
	if err := rows.Err(); err != nil {
		errorLog.Printf("Rows error: %v", err)
	}
	return data, nil
}

func trainAndPredict(ad *ADetector, data []Point) ([]Point, error) {
	infoLog.Printf("Training model %s for forecast horizon %d", ad.ForecastModel, ad.ForecastHorizon)
	if len(data) == 0 {
		errorLog.Printf("No data points to forecast")
		return nil, errors.New("No data points to forecast")
	}
	if ad.ForecastModel != "naive" {
		errorLog.Printf("Unsupported model: %s", ad.ForecastModel)
		return nil, errors.New("Unsupported model: " + ad.ForecastModel)
	}
	lastPoint := data[len(data)-1]
	var forecast []Point
	for i := 1; i <= ad.ForecastHorizon; i++ {
		fcPoint := Point{
			T:     lastPoint.T + int64(i),
			Value: lastPoint.Value,
		}
		forecast = append(forecast, fcPoint)
	}
	infoLog.Printf("Forecast complete, generated %d points", len(forecast))
	return forecast, nil
}

func writeForecast(ad *ADetector, forecast []Point) error {
	destTable := ad.OutputTable
	ctx := context.Background()
	db, err := sql.Open("pgx", ad.ConnectionString)
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

		if _, err := db.ExecContext(ctx, upsertQuery, p.T, p.Value); err != nil {
			errorLog.Printf("Error inserting forecast for %d: %v", p.T, err)
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
	http.HandleFunc("/anomalies", httpForecasts)
	http.HandleFunc("/api/anomalies/update", anomaliesUpdateHandler)
	http.HandleFunc("/api/anomalies/plot", anomaliesPlotHandler)
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
	fData, err := json.Marshal(AD)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "No Forecasts Found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(fData)
}

func anomaliesUpdateHandler(w http.ResponseWriter, r *http.Request) {
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
	ad := AD.ADetectors[id]
	if ad == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: "forecast not found"})
		return
	}
	if err := doForecast(ad); err != nil {
		log.Println("Forecast update error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(Response{Status: "ok"})
}

func anomaliesPlotHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	var ad *ADetector
	if ad = AD.ADetectors[id]; ad == nil {
		http.Error(w, "forecast not found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("pgx", ad.ConnectionString)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "db connection failed", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	selectOrigQuery := fmt.Sprintf(`
		select 
			dt - MIN(dt) OVER () AS ts, 
			views as value 
		from %s
		order by ts asc
	`, ad.DataTable)
	originalRows, err := db.Query(selectOrigQuery)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer originalRows.Close()
	selectForecastQuery := fmt.Sprintf(`
		select 
			step as ts,
			value
		from %s
		order by ts asc
	`, ad.OutputTable)
	forecastRows, err := db.Query(selectForecastQuery)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer forecastRows.Close()

	var original, forecast []Point
	for originalRows.Next() {
		var p Point
		if err := originalRows.Scan(&p.T, &p.Value); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		original = append(original, p)
	}
	for forecastRows.Next() {
		var p Point
		if err := forecastRows.Scan(&p.T, &p.Value); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		forecast = append(forecast, p)
	}

	type PlotResponse struct {
		Original []Point `json:"original"`
		Forecast []Point `json:"forecast"`
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PlotResponse{Original: original, Forecast: forecast})
}
