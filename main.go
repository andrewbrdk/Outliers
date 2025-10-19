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
	Title            string                   `toml:"title"`
	Id               int                      `toml:"-"`
	ConnectionString string                   `toml:"connection"`
	DataSQL          string                   `toml:"data_sql"`
	OutputTable      string                   `toml:"output"`
	Backsteps        int                      `toml:"backsteps"`
	DetectionMethod  string                   `toml:"detection_method"`
	points           map[string][]Point       `toml:"-"`
	markedPoints     map[string][]MarkedPoint `toml:"-"`
	LastUpdate       time.Time                `toml:"-"`
	hasDims          bool                     `toml:"-"`
}

type Point struct {
	T     time.Time
	TUnix int64
	Dim   string
	Value float64
	//todo: Dim int?
}

type MarkedPoint struct {
	T          time.Time `json:"-"`
	TUnix      int64     `json:"T"`
	Dim        string    `json:"Dim"`
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
	// todo: run detection on schedule
	for _, d := range OTL.Detectors {
		err := d.detectOutliers()
		if err != nil {
			errorLog.Printf("Error detecting outliers '%s': %v", d.Title, err)
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
			errorLog.Printf("Skipping invalid detector config at index %d", i)
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
	for _, other := range OTL.Detectors {
		if other != d && other.Title == d.Title {
			errorLog.Printf("Duplicate title found: %s", d.Title)
			return fmt.Errorf("Invalid Config: duplicate title '%s'", d.Title)
		}
	}
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

	err = d.validateColumns(rows)
	if err != nil {
		errorLog.Printf("Column validation error: %v", err)
		return err
	}

	d.points = make(map[string][]Point)
	var p Point
	for rows.Next() {
		if !d.hasDims {
			if err := rows.Scan(&p.T, &p.Value); err != nil {
				errorLog.Printf("Scan error: %v", err)
				continue
			}
			key := ""
			d.points[key] = append(d.points[key], Point{T: p.T, TUnix: p.T.Unix(), Dim: "", Value: p.Value})
		} else {
			if err := rows.Scan(&p.T, &p.Dim, &p.Value); err != nil {
				errorLog.Printf("Scan error: %v", err)
				continue
			}
			key := p.Dim
			d.points[key] = append(d.points[key], Point{T: p.T, TUnix: p.T.Unix(), Dim: p.Dim, Value: p.Value})
		}
	}
	if err := rows.Err(); err != nil {
		errorLog.Printf("Rows error: %v", err)
	}

	for _, p := range d.points {
		sort.Slice(p, func(i, j int) bool {
			return p[i].TUnix < p[j].TUnix
		})
	}
	return nil
}

func (d *Detector) validateColumns(rows *sql.Rows) error {
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
	if len(cols) != 2 && len(cols) != 3 {
		errorLog.Printf("Expected 2 or 3 columns, got %d", len(cols))
		return errors.New("Invalid number of columns")
	}
	if len(cols) == 2 && (cols[0] != "t" || cols[1] != "value") {
		errorLog.Printf("Expecting columns ['t', 'value'], got ['%s', '%s']", cols[0], cols[1])
		return errors.New("Unexpected column names")
	}
	if len(cols) == 3 && (cols[0] != "t" || cols[1] != "dim" || cols[2] != "value") {
		errorLog.Printf("Expecting columns ['t', 'dim', 'value'], got ['%s', '%s', '%s']", cols[0], cols[1], cols[2])
		return errors.New("Unexpected column names")
	}

	tsType := colTypes[0].DatabaseTypeName()
	validTs := tsType == "TIMESTAMP" || tsType == "TIMESTAMPTZ"
	if !validTs {
		errorLog.Printf("Column 't' must be of type timestamp/timestamptz, got '%s'", tsType)
		return errors.New("Column 't' must be of type timestamp/timestamptz")
	}

	if len(cols) == 3 {
		dimType := strings.ToUpper(colTypes[1].DatabaseTypeName())
		if dimType != "TEXT" && dimType != "VARCHAR" {
			errorLog.Printf("Column 'dim' must be of type text/varchar, got '%s'", dimType)
			return errors.New("Column 'dim' must be of type text/varchar")
		}
	}

	var valType string
	if len(cols) == 2 {
		valType = strings.ToUpper(colTypes[1].DatabaseTypeName())
	} else {
		valType = strings.ToUpper(colTypes[2].DatabaseTypeName())
	}
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

	if len(cols) == 3 {
		d.hasDims = true
	} else {
		d.hasDims = false
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

	d.markedPoints = make(map[string][]MarkedPoint)
	totalOutliers := 0

	for dim, pts := range d.points {

		var dimMarked []MarkedPoint
		outlierCount := 0

		for i := 0; i < d.Backsteps; i++ {
			pi := len(pts) - d.Backsteps + i
			sum := 0.0
			for j := 0; j < pi; j++ {
				sum += pts[j].Value
			}

			mean := sum / float64(pi)
			lower := mean * 0.9
			upper := mean * 1.1
			val := pts[pi].Value

			mp := MarkedPoint{
				T:          pts[pi].T,
				TUnix:      pts[pi].TUnix,
				Dim:        dim,
				Value:      val,
				LowerBound: lower,
				UpperBound: upper,
				IsOutlier:  val < lower || val > upper,
			}

			if mp.IsOutlier {
				outlierCount++
			}

			dimMarked = append(dimMarked, mp)
		}

		d.markedPoints[dim] = dimMarked
		totalOutliers += outlierCount

		infoLog.Printf("Dim=%s: detected %d outliers in last %d points", dim, outlierCount, d.Backsteps)
	}
	d.LastUpdate = time.Now()
	infoLog.Printf(
		"Outlier detection complete for %s: total %d outliers detected (method=%s)",
		d.Title, totalOutliers, d.DetectionMethod,
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
			dim text,
			value numeric,
			is_outlier boolean,
			lower_bound numeric,
			upper_bound numeric,
			method text,
			last_update timestamp,
			detector text,
			PRIMARY KEY (t, detector, dim)
		);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_t ON %[1]s (t);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_detector ON %[1]s (detector);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_dim ON %[1]s (dim);`, destTable)
	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		errorLog.Printf("Error creating table %s: %v", destTable, err)
		return err
	}

	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			t, dim, value, is_outlier, lower_bound, upper_bound, method, last_update, detector
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (t, detector, dim)
		DO UPDATE SET
			value = EXCLUDED.value,
			is_outlier = EXCLUDED.is_outlier,
			lower_bound = EXCLUDED.lower_bound,
			upper_bound = EXCLUDED.upper_bound,
			method = EXCLUDED.method,
			last_update = EXCLUDED.last_update;`, destTable)

	stmt, err := db.PrepareContext(ctx, upsertQuery)
	if err != nil {
		errorLog.Printf("Error preparing upsert statement: %v", err)
		return err
	}
	defer stmt.Close()

	for dim, mps := range d.markedPoints {
		for _, mp := range mps {
			_, err := stmt.ExecContext(
				ctx,
				mp.T, dim, mp.Value,
				mp.IsOutlier, mp.LowerBound, mp.UpperBound,
				d.DetectionMethod, d.LastUpdate, d.Title)
			if err != nil {
				errorLog.Printf("Error inserting dim=%s t=%v: %v", dim, mp.T, err)
				return err
			}
		}
	}

	infoLog.Printf("Results written to table %s for %s (%d dims)", destTable, d.Title, len(d.markedPoints))
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
