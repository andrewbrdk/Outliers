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

var OTL Outliers
var CONF Config

type Outliers struct {
	Detectors map[int]*Detector
	counter   int
	Parsed    []*Detector `toml:"detectors"`
}

type Config struct {
	port     string
	confFile string
}

type Detector struct {
	Title            string        `toml:"title"`
	Id               int           `toml:"-"`
	ConnectionString string        `toml:"connection"`
	DataTable        string        `toml:"source"`
	OutputTable      string        `toml:"output"`
	Backsteps        int           `toml:"backsteps"`
	DetectionMethod  string        `toml:"detection_method"`
	points           []Point       `toml:"-"`
	markedPoints     []MarkedPoint `toml:"-"`
}

type Point struct {
	T     int64
	Value float64
}

type MarkedPoint struct {
	T         int64
	Value     float64
	IsOutlier bool
}

func main() {
	initConfig()
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	OTL.loadDetectors(CONF.confFile)
	// todo: run forecasts on schedule
	for _, d := range OTL.Detectors {
		err := d.detectOutliers()
		if err != nil {
			errorLog.Printf("Error doing forecast '%s': %v", d.Title, err)
		}
	}
	httpServer()
}

func initConfig() {
	CONF.port = ":9090"
	CONF.confFile = "outliers.toml"
	if port := os.Getenv("OUTLIERS_PORT"); port != "" {
		CONF.port = ":" + port
	}
	if confFile := os.Getenv("OUTLIERS_CONF"); confFile != "" {
		CONF.confFile = confFile
	}
}

func (ot *Outliers) loadDetectors(filename string) error {
	f, err := os.ReadFile(filename)
	if err != nil {
		errorLog.Printf("Error reading file %s: %v\n", filename, err)
		return err
	}
	err = toml.Unmarshal(f, ot)
	if err != nil {
		errorLog.Printf("Error parsing file %s: %v\n", filename, err)
		return err
	}
	ot.Detectors = make(map[int]*Detector)
	for i, d := range ot.Parsed {
		err := d.validateConf()
		if err != nil {
			errorLog.Printf("Skipping invalid config at index %d", i)
			continue
		}
		ot.Detectors[ot.counter] = d
		d.Id = ot.counter
		ot.counter += 1
	}
	infoLog.Printf("Loaded %d configs from %s", len(ot.Detectors), filename)
	return nil
}

func (d *Detector) validateConf() error {
	if d.Title == "" {
		errorLog.Printf("Config missing title")
		return errors.New("Invalid Config: missing title")
	}
	if d.ConnectionString == "" {
		errorLog.Printf("Config '%s' missing connection string", d.Title)
		return errors.New("Invalid Config: missing connection string")
	}
	if d.DataTable == "" {
		errorLog.Printf("Config '%s' missing source table", d.Title)
		return errors.New("Invalid Config: missing source table")
	}
	if d.OutputTable == "" {
		errorLog.Printf("Config '%s' missing output table", d.Title)
		return errors.New("Invalid Config: missing output table")
	}
	if d.Backsteps <= 0 {
		errorLog.Printf("Invalid backsteps: %d", d.Backsteps)
		return fmt.Errorf("invalid backsteps: %d", d.Backsteps)
	}
	return nil
}

func (d *Detector) detectOutliers() error {
	infoLog.Printf("Detecting %s", d.Title)
	err := d.readTimeSeries()
	if err != nil {
		return err
	}
	err = d.markOutliers()
	if err != nil {
		return err
	}
	err = d.writeResults()
	if err != nil {
		return err
	}
	return nil
}

func (d *Detector) readTimeSeries() error {
	selectQuery := fmt.Sprintf(`
		select
			dt - MIN(dt) OVER () AS ts, 
			views as value
		from %s
		order by ts asc
	`, d.DataTable)
	ctx := context.Background()
	db, err := sql.Open("pgx", d.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, selectQuery)
	if err != nil {
		errorLog.Printf("Error reading from table: %v", err)
		return err
	}
	defer rows.Close()

	//todo: simplify
	var p Point
	for rows.Next() {
		if err := rows.Scan(&p.T, &p.Value); err != nil {
			errorLog.Printf("Scan error: %v", err)
		}
		d.points = append(d.points, Point{T: p.T, Value: p.Value})
	}
	if err := rows.Err(); err != nil {
		errorLog.Printf("Rows error: %v", err)
	}
	return nil
}

func (d *Detector) markOutliers() error {
	infoLog.Printf("Detecting outliers for %s", d.Title)
	if len(d.points) == 0 {
		errorLog.Printf("No data")
		return errors.New("No data")
	}
	if d.DetectionMethod != "mean10prc" {
		errorLog.Printf("Unsupported detection method: %s", d.DetectionMethod)
		return errors.New("Unsupported detection method: " + d.DetectionMethod)
	}

	d.markedPoints = make([]MarkedPoint, d.Backsteps)
	outlierCount := 0
	for i := 0; i < d.Backsteps; i++ {
		sum := 0.0
		pi := len(d.points) - d.Backsteps + i
		for j := 0; j < pi; j++ {
			sum += d.points[j].Value
		}
		mean := sum / float64(pi)
		lower := mean * 0.9
		upper := mean * 1.1
		val := d.points[pi].Value
		d.markedPoints[i].T = d.points[pi].T
		d.markedPoints[i].Value = val
		d.markedPoints[i].IsOutlier = false
		if val < lower || val > upper {
			d.markedPoints[i].IsOutlier = true
			outlierCount++
		}
	}
	infoLog.Printf(
		"Outlier detection complete: %d outliers detected in last %d points (method=%s)",
		outlierCount, d.Backsteps, d.DetectionMethod,
	)
	return nil
}

func (d *Detector) writeResults() error {
	destTable := d.OutputTable
	ctx := context.Background()
	db, err := sql.Open("pgx", d.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
	}
	defer db.Close()

	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			dt DATE,
			step int,
			value numeric,
			is_outlier boolean,
			detection_method text,
			detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(step, detection_method)
		)`, destTable)
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		errorLog.Printf("Error creating table %s: %v", destTable, err)
		return err
	}

	for _, mp := range d.markedPoints {
		upsertQuery := fmt.Sprintf(`
			INSERT INTO %s (dt, step, value, is_outlier, detection_method, detected_at) 
			VALUES (NULL, $1, $2, $3, $4, CURRENT_TIMESTAMP)
			ON CONFLICT (step, detection_method)
			DO UPDATE SET
				value = EXCLUDED.value,
				is_outlier = EXCLUDED.is_outlier,
				detected_at = EXCLUDED.detected_at
		`, destTable)

		if _, err := db.ExecContext(ctx, upsertQuery, mp.T, mp.Value, mp.IsOutlier, d.DetectionMethod); err != nil {
			errorLog.Printf("Error inserting outlier for %d: %v", mp.T, err)
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
	http.HandleFunc("/outliers", httpOutliers)
	http.HandleFunc("/outliers/update", outliersUpdateHandler)
	http.HandleFunc("/outliers/plot", outliersPlotHandler)
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

func httpOutliers(w http.ResponseWriter, r *http.Request) {
	fData, err := json.Marshal(OTL)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "No Forecasts Found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(fData)
}

func outliersUpdateHandler(w http.ResponseWriter, r *http.Request) {
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
	d := OTL.Detectors[id]
	if d == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: "forecast not found"})
		return
	}
	if err := d.detectOutliers(); err != nil {
		log.Println("Forecast update error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(Response{Status: "ok"})
}

func outliersPlotHandler(w http.ResponseWriter, r *http.Request) {
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

	var d *Detector
	if d = OTL.Detectors[id]; d == nil {
		http.Error(w, "Detector not found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("pgx", d.ConnectionString)
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
	`, d.DataTable)
	originalRows, err := db.Query(selectOrigQuery)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer originalRows.Close()
	selectOutliersQuery := fmt.Sprintf(`
		select 
			step as ts,
			value,
			is_outlier
		from %s
		order by ts asc
	`, d.OutputTable)
	outliersRows, err := db.Query(selectOutliersQuery)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer outliersRows.Close()

	var p Point
	var original []Point
	for originalRows.Next() {
		if err := originalRows.Scan(&p.T, &p.Value); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		original = append(original, p)
	}
	var mp MarkedPoint
	var outliers []MarkedPoint
	for outliersRows.Next() {
		if err := outliersRows.Scan(&mp.T, &mp.Value, &mp.IsOutlier); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		if mp.IsOutlier {
			outliers = append(outliers, mp)
		}
	}

	type PlotResponse struct {
		Original []Point       `json:"original"`
		Outliers []MarkedPoint `json:"outliers"`
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PlotResponse{Original: original, Outliers: outliers})
}
