package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	_ "github.com/jackc/pgx/v5/stdlib"
	hcron "github.com/lnquy/cron"
	"github.com/robfig/cron/v3"
)

//go:embed index.html style.css
var embedded embed.FS

var infoLog *log.Logger
var errorLog *log.Logger

var OTL Outliers
var CONF Config

var Connections = make(map[string]*Connection)
var Notifiers = make(map[string]Notifier)

type Outliers struct {
	Detectors map[int]*Detector
	counter   int
	cron      *cron.Cron
	mu        sync.RWMutex
}

type Config struct {
	port     string
	confFile string
}

type Detector struct {
	Title            string                   `toml:"title"`
	Id               int                      `toml:"-"`
	ConnectionName   string                   `toml:"connection"`
	DataSQL          string                   `toml:"data_sql"`
	OutputTable      string                   `toml:"output"`
	Backsteps        int                      `toml:"backsteps"`
	DetectionMethod  string                   `toml:"detection_method"`
	points           map[string][]Point       `toml:"-"`
	markedPoints     map[string][]MarkedPoint `toml:"-"`
	LastUpdate       time.Time                `toml:"-"`
	hasDims          bool                     `toml:"-"`
	TotalOutliers    int                      `toml:"-"`
	DimsWithOutliers int                      `toml:"-"`
	CronSchedule     string                   `toml:"cron_schedule"`
	HCron            string                   `toml:"-"`
	cronID           cron.EntryID             `toml:"-"`
	NextScheduled    time.Time                `toml:"-"`
	OnOff            bool                     `toml:"-"`
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
	//todo: parse all config at once
	err := loadConnections(CONF.confFile)
	if err != nil {
		errorLog.Println("Error loading connections:", err)
	}
	err = loadNotifiers(CONF.confFile)
	if err != nil {
		errorLog.Println("Error loading notifiers:", err)
	}
	OTL.cron = cron.New()
	OTL.loadDetectors(CONF.confFile)
	OTL.cron.Start()
	go startFSWatcher()
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

func startFSWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = watcher.Add(CONF.confFile)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				//todo: prevent multiple triggers
				go func() {
					time.Sleep(300 * time.Millisecond) //prevents reading incomplete file
					OTL.loadDetectors(CONF.confFile)
				}()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			errorLog.Println("fsnotify:", err)
		}
	}
}

func (ot *Outliers) loadDetectors(filename string) error {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	f, err := os.ReadFile(filename)
	if err != nil {
		errorLog.Printf("Error reading file %s: %v", filename, err)
		return err
	}
	type OutliersConf struct {
		Parsed []*Detector `toml:"detectors"`
	}
	var tmp OutliersConf
	if err := toml.Unmarshal(f, &tmp); err != nil {
		errorLog.Printf("Error parsing file %s: %v", filename, err)
		return err
	}
	confDetectors := make(map[string]*Detector)
	for _, d := range tmp.Parsed {
		err = d.validateConf()
		if err != nil {
			errorLog.Printf("Skipping invalid config in %s: %v", d.Title, err)
			continue
		}
		confDetectors[d.Title] = d
		infoLog.Printf("Validated config for detector '%s'", d.Title)
	}

	if ot.Detectors == nil {
		ot.Detectors = make(map[int]*Detector)
	}
	currentDetectors := make(map[string]*Detector)
	for _, d := range ot.Detectors {
		currentDetectors[d.Title] = d
	}

	for title, d := range confDetectors {
		if existing, ok := currentDetectors[title]; ok {
			if noConfChanges(existing, d) {
				infoLog.Printf("No changes for detector '%s', skipping reload", d.Title)
				continue
			}
			ot.cron.Remove(existing.cronID)
			//todo: stop sqls
			delete(ot.Detectors, existing.Id)
			infoLog.Printf("Reloading detector '%s' due to config changes", d.Title)
		} else {
			infoLog.Printf("Loading new detector '%s'", d.Title)
		}
		d.Id = ot.counter
		d.OnOff = false
		ot.Detectors[ot.counter] = d
		if d.CronSchedule != "" {
			ot.scheduleDetectorUpdate(d)
		}
		ot.counter++
	}

	for id, existing := range ot.Detectors {
		if _, ok := confDetectors[existing.Title]; !ok {
			ot.cron.Remove(existing.cronID)
			//todo: stop sqls
			delete(ot.Detectors, id)
			infoLog.Printf("Removed detector '%s' (id=%d, not in config)", existing.Title, id)
		}
	}
	infoLog.Printf("Configured %d detectors from %s", len(ot.Detectors), filename)
	return nil
}

func (d *Detector) validateConf() error {
	if d.Title == "" {
		errorLog.Printf("Config missing title")
		return errors.New("Invalid Config: missing title")
	}
	if d.ConnectionName == "" {
		errorLog.Printf("Config '%s' missing connection name", d.Title)
		return errors.New("Invalid Config: missing connection name")
	}
	//todo: check connection exists
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

func noConfChanges(d1 *Detector, d2 *Detector) bool {
	return d1.DataSQL == d2.DataSQL &&
		d1.OutputTable == d2.OutputTable &&
		d1.Backsteps == d2.Backsteps &&
		d1.DetectionMethod == d2.DetectionMethod &&
		d1.CronSchedule == d2.CronSchedule &&
		d1.ConnectionName == d2.ConnectionName
}

func (ot *Outliers) scheduleDetectorUpdate(d *Detector) {
	var err error
	exprDesc, _ := hcron.NewDescriptor(hcron.Use24HourTimeFormat(true))
	d.HCron, err = exprDesc.ToDescription(d.CronSchedule, hcron.Locale_en)
	if d.CronSchedule == "" {
		d.HCron = ""
	} else if err != nil {
		d.HCron = d.CronSchedule
	}
	if !d.OnOff {
		return
	}
	d.cronID, err = ot.cron.AddFunc(d.CronSchedule, func() {
		infoLog.Printf("Scheduled update for detector '%s' started", d.Title)
		err := d.detectOutliers()
		if err != nil {
			errorLog.Printf("Error detecting outliers '%s': %v", d.Title, err)
		}
		d.NextScheduled = ot.cron.Entry(d.cronID).Next
	})
	if err != nil {
		errorLog.Printf("Error scheduling detector '%s' update: %v", d.Title, err)
		return
	}
	d.NextScheduled = ot.cron.Entry(d.cronID).Next
	infoLog.Printf("Scheduled outlier detection for %s with cron %s", d.Title, d.CronSchedule)
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
	d.notify()
	return nil
}

func (d *Detector) readTimeSeries() error {
	db, err := GetDB(d.ConnectionName)
	if err != nil {
		return fmt.Errorf("detector %s: %v", d.Title, err)
	}

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
	d.TotalOutliers = 0
	d.DimsWithOutliers = 0

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
		d.TotalOutliers += outlierCount
		if outlierCount > 0 {
			d.DimsWithOutliers++
		}

		infoLog.Printf("Dim=%s: detected %d outliers in last %d points", dim, outlierCount, d.Backsteps)
	}
	d.LastUpdate = time.Now()
	infoLog.Printf(
		"Outlier detection complete for %s: total %d outliers detected (method=%s)",
		d.Title, d.TotalOutliers, d.DetectionMethod,
	)
	return nil
}

func (d *Detector) writeResults() error {
	destTable := d.OutputTable

	db, err := GetDB(d.ConnectionName)
	if err != nil {
		errorLog.Printf("Error getting DB connection: %v", err)
		return fmt.Errorf("detector %s: failed to get connection: %v", d.Title, err)
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

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

func (d *Detector) notify() error {
	if d.TotalOutliers == 0 {
		return nil
	}
	msg := fmt.Sprintf("%s Detector '%s' found %d outliers.", d.LastUpdate.Format("2006-01-02 15:04"), d.Title, d.TotalOutliers)
	for _, notifier := range Notifiers {
		err := notifier.Notify(msg)
		if err != nil {
			errorLog.Printf("Error sending notification for '%s': %v", d.Title, err)
		} else {
			infoLog.Printf("Notification sent for detector '%s'", d.Title)
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
	http.Handle("/style.css", http.FileServer(http.FS(embedded)))
	http.HandleFunc("/outliers", httpOutliers)
	http.HandleFunc("/outliers/update", outliersUpdateHandler)
	http.HandleFunc("/outliers/plot", outliersPlotHandler)
	http.HandleFunc("/outliers/onoff", outliersOnOffHandler)
	http.HandleFunc("/notifications", httpListNotifications)
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
	fData, err := json.Marshal(&OTL)
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

	db, err := GetDB(d.ConnectionName)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "db connection failed", http.StatusInternalServerError)
		return
	}

	selectOutliersQuery := fmt.Sprintf(`
		select 
			t,
			dim,
			value,
			is_outlier,
			lower_bound,
			upper_bound
		from %s
		where detector = $1
		order by dim ASC, t ASC
	`, d.OutputTable)
	rows, err := db.Query(selectOutliersQuery, d.Title)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var mp MarkedPoint
	outliersByDim := make(map[string][]MarkedPoint)
	for rows.Next() {
		if err := rows.Scan(&mp.T, &mp.Dim, &mp.Value, &mp.IsOutlier, &mp.LowerBound, &mp.UpperBound); err != nil {
			errorLog.Println(err)
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		mp.TUnix = mp.T.Unix()
		outliersByDim[mp.Dim] = append(outliersByDim[mp.Dim], mp)
	}
	if err := rows.Err(); err != nil {
		errorLog.Printf("Rows iteration error: %v", err)
		http.Error(w, "rows error", http.StatusInternalServerError)
		return
	}

	type PlotResponse struct {
		Outliers map[string][]MarkedPoint `json:"outliers"`
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(PlotResponse{Outliers: outliersByDim})
	if err != nil {
		errorLog.Printf("Encoding error: %v", err)
		http.Error(w, "encode failed", http.StatusInternalServerError)
	}
}

func outliersOnOffHandler(w http.ResponseWriter, r *http.Request) {
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
	if d.OnOff {
		OTL.cron.Remove(d.cronID)
		d.OnOff = false
		d.NextScheduled = time.Time{}
		infoLog.Printf("Detector '%s' turned OFF", d.Title)
	} else {
		d.OnOff = true
		OTL.scheduleDetectorUpdate(d)
		infoLog.Printf("Detector '%s' turned ON", d.Title)
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

func httpListNotifications(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	data := make([]map[string]string, 0)

	for title, n := range Notifiers {
		switch nt := n.(type) {
		case *SlackNotification:
			data = append(data, map[string]string{
				"title": nt.Title,
				"type":  "Slack",
				"url":   nt.WebhookURL,
			})
		case *EmailNotification:
			data = append(data, map[string]string{
				"title":    nt.Title,
				"type":     "Email",
				"smtp":     nt.SMTPServer,
				"username": nt.Username,
				//"to":       nt.To,
			})
		default:
			data = append(data, map[string]string{
				"title": title,
				"type":  "Unknown",
			})
		}
	}

	json.NewEncoder(w).Encode(data)
}

type Notifier interface {
	Notify(message string) error
}

type SlackNotification struct {
	Title      string
	WebhookURL string
}

type EmailNotification struct {
	Title      string
	SMTPServer string
	Username   string
	Password   string
	From       string
	To         []string
}

type NotificationConfig struct {
	Title      string   `toml:"title"`
	Type       string   `toml:"type"`
	WebhookURL string   `toml:"webhook_url"`
	SMTPServer string   `toml:"smtp_server"`
	Username   string   `toml:"username"`
	Password   string   `toml:"password"`
	From       string   `toml:"from"`
	To         []string `toml:"to"`
}

func loadNotifiers(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %v", filename, err)
	}
	var confs struct {
		Notifications []NotificationConfig `toml:"notifications"`
	}
	if err := toml.Unmarshal(data, &confs); err != nil {
		return fmt.Errorf("error parsing TOML %s: %v", filename, err)
	}
	Notifiers = make(map[string]Notifier)
	for _, n := range confs.Notifications {
		//todo: warning on overwrite
		switch n.Type {
		case "slack":
			Notifiers[n.Title] = &SlackNotification{
				Title:      n.Title,
				WebhookURL: n.WebhookURL,
			}
		case "email":
			Notifiers[n.Title] = &EmailNotification{
				Title:      n.Title,
				SMTPServer: n.SMTPServer,
				Username:   n.Username,
				Password:   n.Password,
				From:       n.From,
				To:         n.To,
			}
		default:
			fmt.Printf("Unknown notifier type '%s' for '%s'\n", n.Type, n.Title)
		}
	}

	fmt.Printf("Loaded %d notifiers\n", len(Notifiers))
	return nil
}

func (s *SlackNotification) Notify(message string) error {
	if s.WebhookURL == "" {
		return fmt.Errorf("no Slack webhook configured for %s", s.Title)
	}
	payload := map[string]string{"text": message}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(s.WebhookURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	return nil
}

func (e *EmailNotification) Notify(message string) error {
	if e.SMTPServer == "" {
		return fmt.Errorf("no SMTP server configured for %s", e.Title)
	}

	auth := smtp.PlainAuth("", e.Username, e.Password, e.SMTPServer)
	return smtp.SendMail(e.SMTPServer, auth, e.From, e.To, []byte(message))
}

type Connection struct {
	Title           string  `toml:"title"`
	Type            string  `toml:"type"`
	ConnStr         string  `toml:"connection"`
	MaxOpenConns    int     `toml:"max_open_conns"`
	MaxIdleConns    int     `toml:"max_idle_conns"`
	ConnMaxLifetime string  `toml:"conn_max_lifetime"`
	DB              *sql.DB `toml:"-"`
}

type ConnectionConfig struct {
	Title           string `toml:"title"`
	Type            string `toml:"type"`
	ConnStr         string `toml:"connection"`
	MaxOpenConns    int    `toml:"max_open_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"`
}

func loadConnections(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %v", filename, err)
	}

	var confs struct {
		Connections []ConnectionConfig `toml:"connections"`
	}
	if err := toml.Unmarshal(data, &confs); err != nil {
		return fmt.Errorf("error parsing TOML %s: %v", filename, err)
	}

	_ = CloseAllConnections()
	Connections = make(map[string]*Connection)

	for _, c := range confs.Connections {
		if c.Title == "" || c.Type == "" || c.ConnStr == "" {
			errorLog.Printf("Skipping invalid connection entry (missing title/type/connection)")
			continue
		}
		if c.Type != "postgres" {
			errorLog.Printf("Skipping connection '%s': unsupported type '%s'", c.Title, c.Type)
			continue
		}
		driver := "pgx"
		db, err := sql.Open(driver, c.ConnStr)
		if err != nil {
			errorLog.Printf("Failed to open DB for %s: %v", c.Title, err)
			continue
		}
		if c.MaxOpenConns > 0 {
			db.SetMaxOpenConns(c.MaxOpenConns)
		}
		if c.MaxIdleConns > 0 {
			db.SetMaxIdleConns(c.MaxIdleConns)
		}
		if c.ConnMaxLifetime != "" {
			if d, err := time.ParseDuration(c.ConnMaxLifetime); err == nil {
				db.SetConnMaxLifetime(d)
			} else {
				errorLog.Printf("Invalid conn_max_lifetime for %s: %v (using default)", c.Title, err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err != nil {
			errorLog.Printf("DB ping failed for %s: %v", c.Title, err)
			db.Close()
			continue
		}

		con := &Connection{
			Title:           c.Title,
			Type:            c.Type,
			ConnStr:         c.ConnStr,
			MaxOpenConns:    c.MaxOpenConns,
			MaxIdleConns:    c.MaxIdleConns,
			ConnMaxLifetime: c.ConnMaxLifetime,
			DB:              db,
		}

		Connections[c.Title] = con
		infoLog.Printf("Opened connection '%s' (type=%s)", c.Title, c.Type)
	}

	infoLog.Printf("Loaded %d connections from %s", len(Connections), filename)
	return nil
}

func CloseAllConnections() error {
	var firstErr error
	for k, c := range Connections {
		if c != nil && c.DB != nil {
			if err := c.DB.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			infoLog.Printf("Closed connection '%s'", k)
		}
		delete(Connections, k)
	}
	return firstErr
}

func GetDB(name string) (*sql.DB, error) {
	conn, ok := Connections[name]
	if ok && conn != nil && conn.DB != nil {
		return conn.DB, nil
	}
	return nil, fmt.Errorf("connection %q not found or not initialized", name)
}
