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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

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
	DataSQL          string        `toml:"data_sql"`
	OutputTable      string        `toml:"output"`
	Backsteps        int           `toml:"backsteps"`
	DetectionMethod  string        `toml:"detection_method"`
	points           []Point       `toml:"-"`
	markedPoints     []MarkedPoint `toml:"-"`
	LastUpdate       time.Time     `toml:"-"`
}

type Point struct {
	T     time.Time
	Value float64
}

type MarkedPoint struct {
	T          time.Time `json:"-"`
	TUnix      int64     `json:"T"`
	Value      float64   `json:"Value"`
	IsOutlier  bool      `json:"IsOutlier"`
	LowerBound float64   `json:"LowerBound"`
	UpperBound float64   `json:"UpperBound"`
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
	//todo: check unique title
	if d.ConnectionString == "" {
		errorLog.Printf("Config '%s' missing connection string", d.Title)
		return errors.New("Invalid Config: missing connection string")
	}
	if d.DataSQL == "" {
		errorLog.Printf("Config '%s' missing data_sql", d.Title)
		return errors.New("Invalid Config: missing data_sql")
	}
	if d.OutputTable == "" {
		errorLog.Printf("Config '%s' missing output table", d.Title)
		return errors.New("Invalid Config: missing output table")
	}
	var validTableName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)
	if !validTableName.MatchString(d.OutputTable) {
		errorLog.Printf("Invalid output table name: %s", d.OutputTable)
		return errors.New("invalid output table name: " + d.OutputTable)
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
	defer func() { d.points = nil }()
	if err != nil {
		return err
	}
	err = d.markOutliers()
	defer func() { d.markedPoints = nil }()
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
	db, err := sql.Open("pgx", d.ConnectionString)
	if err != nil {
		errorLog.Printf("Error connecting to Postgres: %v", err)
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3600*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, "SET default_transaction_read_only = on;")
	if err != nil {
		errorLog.Printf("Failed to set read-only: %v", err)
		return err
	}

	rows, err := db.QueryContext(ctx, d.DataSQL)
	if err != nil {
		errorLog.Printf("Error reading from table: %v", err)
		return err
	}
	defer rows.Close()

	err = validateColumns(rows)
	if err != nil {
		errorLog.Printf("Column validation error: %v", err)
		return err
	}

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

	sort.Slice(d.points, func(i, j int) bool {
		return d.points[i].T.Before(d.points[j].T)
	})
	return nil
}

func validateColumns(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		errorLog.Printf("Cannot get columns: %v", err)
		return errors.New("Cannot get columns")
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		errorLog.Printf("Cannot get column types: %v", err)
		return errors.New("Cannot get column types")
	}
	if len(cols) != 2 {
		errorLog.Printf("Expected 2 columns (t, value), got %d", len(cols))
		return errors.New("Invalid number of columns")
	}
	if cols[0] != "t" {
		errorLog.Printf("First column must be named 't', got '%s'", cols[0])
		return errors.New("First column must be named 't'")
	}
	if cols[1] != "value" {
		errorLog.Printf("Second column must be named 'value', got '%s'", cols[1])
		return errors.New("Second column must be named 'value'")
	}

	tsType := colTypes[0].DatabaseTypeName()
	validTs := tsType == "TIMESTAMP" || tsType == "TIMESTAMPTZ"
	if !validTs {
		errorLog.Printf("Column 't' must be of type timestamp/timestamptz, got '%s'", tsType)
		return errors.New("Column 't' must be of type timestamp/timestamptz")
	}

	valType := strings.ToUpper(colTypes[1].DatabaseTypeName())
	allowedValType := map[string]bool{
		"NUMERIC": true,
		"DECIMAL": true,
		"INT":     true,
		"INT2":    true,
		"INT4":    true,
		"INT8":    true,
		"FLOAT4":  true,
		"FLOAT8":  true,
		"DOUBLE":  true,
		"REAL":    true,
	}
	if !allowedValType[valType] {
		errorLog.Printf("Column 'value' must be numeric or int, got '%s'", valType)
		return errors.New("Column 'value' must be numeric or int")
	}
	return nil
}

func (d *Detector) markOutliers() error {
	infoLog.Printf("Marking outliers for %s", d.Title)
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
		d.markedPoints[i].LowerBound = lower
		d.markedPoints[i].UpperBound = upper
		if val < lower || val > upper {
			d.markedPoints[i].IsOutlier = true
			outlierCount++
		}
	}
	d.LastUpdate = time.Now()
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
			t timestamp,
			value numeric,
			is_outlier boolean,
			lower_bound numeric,
			upper_bound numeric,
			method text,
			last_update timestamp,
			detector text,
			PRIMARY KEY (t, detector)
		)`, destTable)
	// todo: add indexes
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		errorLog.Printf("Error creating table %s: %v", destTable, err)
		return err
	}

	for _, mp := range d.markedPoints {
		upsertQuery := fmt.Sprintf(`
			INSERT INTO %s (t, value, is_outlier, lower_bound, upper_bound, method, last_update, detector) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (t, detector)
			DO UPDATE SET
				value = EXCLUDED.value,
				is_outlier = EXCLUDED.is_outlier,
				lower_bound = EXCLUDED.lower_bound,
				upper_bound = EXCLUDED.upper_bound,
				last_update = EXCLUDED.last_update
		`, destTable)

		if _, err := db.ExecContext(ctx, upsertQuery, mp.T, mp.Value, mp.IsOutlier, mp.LowerBound, mp.UpperBound, d.DetectionMethod, d.LastUpdate, d.Title); err != nil {
			errorLog.Printf("Error inserting outlier: %v", err)
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
		http.Error(w, "Internal error reading data", http.StatusNotFound)
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
		errorLog.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Status   string    `json:"status"`
		Detector *Detector `json:"detector"`
	}{
		Status:   "ok",
		Detector: d,
	})
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

	selectOutliersQuery := fmt.Sprintf(`
		select 
			t,
			value,
			is_outlier,
			lower_bound,
			upper_bound
		from %s
		where detector = $1
		order by t asc
	`, d.OutputTable)
	outliersRows, err := db.Query(selectOutliersQuery, d.Title)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer outliersRows.Close()

	var mp MarkedPoint
	var outliers []MarkedPoint
	for outliersRows.Next() {
		if err := outliersRows.Scan(&mp.T, &mp.Value, &mp.IsOutlier, &mp.LowerBound, &mp.UpperBound); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		mp.TUnix = mp.T.Unix()
		outliers = append(outliers, mp)
	}

	type PlotResponse struct {
		Outliers []MarkedPoint `json:"outliers"`
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PlotResponse{Outliers: outliers})
}
