package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
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
	"github.com/golang-jwt/jwt/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	hcron "github.com/lnquy/cron"
	"github.com/robfig/cron/v3"
)

//go:embed index.html style.css
var embedded embed.FS

var jwtSecretKey []byte

var infoLog *log.Logger
var errorLog *log.Logger

var OTL Outliers
var CONF Config

type Outliers struct {
	Connections map[string]Connection
	Notifiers   map[string]Notifier
	Detectors   map[int]*Detector
	counter     int
	cron        *cron.Cron
	mu          sync.RWMutex
	parsedConf  ParsedConfig
}

type Config struct {
	port     string
	confFile string
	password string
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
	DimsOutliers     map[string]int           `toml:"-"`
	DimsWithOutliers int                      `toml:"-"`
	CronSchedule     string                   `toml:"cron_schedule"`
	HCron            string                   `toml:"-"`
	cronID           cron.EntryID             `toml:"-"`
	NextScheduled    time.Time                `toml:"-"`
	OnOff            bool                     `toml:"-"`
	NotifyEmails     []string                 `toml:"notify_emails"`
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

type Connection interface {
	GetDB() *sql.DB
	Close() error
}

type ConnectionConfig struct {
	Title           string `toml:"title"`
	Type            string `toml:"type"`
	ConnStr         string `toml:"connection"`
	MaxOpenConns    int    `toml:"max_open_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"`
}

type PostgresConnection struct {
	Title           string
	ConnStr         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime string
	DB              *sql.DB
}

type Notifier interface {
	Notify(message string, d *Detector) error
	GetTitle() string
}

type NotificationConfig struct {
	Title              string   `toml:"title"`
	Type               string   `toml:"type"`
	WebhookURL         string   `toml:"webhook_url"`
	SMTPServerWithPort string   `toml:"smtp_server_with_port"`
	Username           string   `toml:"username"`
	Password           string   `toml:"password"`
	From               string   `toml:"from"`
	CommonRecipients   []string `toml:"common_recipients"`
}

type SlackNotification struct {
	Title      string
	WebhookURL string
}

type EmailNotification struct {
	Title              string
	SMTPServerWithPort string
	Username           string
	Password           string
	From               string
	CommonRecipients   []string
}

type ParsedConfig struct {
	//todo: make uniform
	Connections   []ConnectionConfig   `toml:"connections"`
	Notifications []NotificationConfig `toml:"notifications"`
	Detectors     []*Detector          `toml:"detectors"`
}

func main() {
	initConfig()
	jwtSecretKey = generateRandomKey(32)
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	err := OTL.loadConfFile(CONF.confFile)
	if err != nil {
		errorLog.Println("Error loading config:", err)
	}
	OTL.initConnections()
	OTL.initNotifiers()
	OTL.cron = cron.New()
	OTL.initDetectors()
	OTL.cron.Start()
	go startFSWatcher()
	httpServer()
}

func initConfig() {
	CONF.port = ":9090"
	CONF.confFile = "outliers.toml"
	CONF.password = ""
	if port := os.Getenv("OUTLIERS_PORT"); port != "" {
		CONF.port = ":" + port
	}
	if confFile := os.Getenv("OUTLIERS_CONF"); confFile != "" {
		CONF.confFile = confFile
	}
	CONF.password = os.Getenv("OUTLIERS_PASSWORD")
}

func generateRandomKey(size int) []byte {
	key := make([]byte, size)
	_, err := rand.Read(key)
	if err != nil {
		errorLog.Printf("Failed to generate a JWT secret key. Aborting.")
		os.Exit(1)
	}
	return key
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
					err := OTL.loadConfFile(CONF.confFile)
					if err != nil {
						errorLog.Println("Error loading config:", err)
					}
					OTL.initConnections()
					OTL.initNotifiers()
					OTL.initDetectors()
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

func (ot *Outliers) loadConfFile(filename string) error {
	f, err := os.ReadFile(filename)
	if err != nil {
		errorLog.Printf("Error reading file %s: %v", filename, err)
		return err
	}
	if err := toml.Unmarshal(f, &ot.parsedConf); err != nil {
		errorLog.Printf("Error parsing file %s: %v", filename, err)
		return err
	}
	return nil
}

func (ot *Outliers) initDetectors() error {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	var err error

	confDetectors := make(map[string]*Detector)
	for _, d := range ot.parsedConf.Detectors {
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
	infoLog.Printf("Configured %d detectors", len(ot.Detectors))
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
		go func() {
			infoLog.Printf("Scheduled update for detector '%s' started", d.Title)
			err := d.detectOutliers()
			if err != nil {
				errorLog.Printf("Error detecting outliers '%s': %v", d.Title, err)
			}
			d.NextScheduled = ot.cron.Entry(d.cronID).Next
			broadcastSSEUpdate(fmt.Sprintf(`{"status":"completed", "detector":"%d"}`, d.Id))
		}()
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
	db, err := OTL.GetDB(d.ConnectionName)
	if err != nil {
		return fmt.Errorf("detector %s: %v", d.Title, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3600*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("detector %s: begin readonly tx failed: %v", d.Title, err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, d.DataSQL)
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

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("detector %s: commit failed: %v", d.Title, err)
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
	d.DimsOutliers = make(map[string]int)
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
		d.DimsOutliers[dim] = outlierCount
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

	db, err := OTL.GetDB(d.ConnectionName)
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
	var msg string
	if !d.hasDims {
		msg = fmt.Sprintf(
			"%s Detector '%s' has found %d outliers.",
			d.LastUpdate.Format("2006-01-02 15:04"),
			d.Title,
			d.TotalOutliers,
		)
	} else {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf(
			"%s Detector '%s' has found %d outliers in %d series:\n",
			d.LastUpdate.Format("2006-01-02 15:04"),
			d.Title,
			d.TotalOutliers,
			d.DimsWithOutliers,
		))
		for dim, count := range d.DimsOutliers {
			sb.WriteString(fmt.Sprintf("- %s: %d outliers\n", dim, count))
		}
		msg = sb.String()
	}
	for _, n := range OTL.Notifiers {
		go func(notifier Notifier) {
			err := notifier.Notify(msg, d)
			if err != nil {
				errorLog.Printf("Error sending %s notification for '%s': %v", notifier.GetTitle(), d.Title, err)
			} else {
				infoLog.Printf("Sent %s notification for detector '%s'", notifier.GetTitle(), d.Title)
			}
		}(n)
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
	http.HandleFunc("/login", httpLogin)
	http.HandleFunc("/outliers", httpOutliers)
	http.HandleFunc("/outliers/update", outliersUpdateHandler)
	http.HandleFunc("/outliers/plot", outliersPlotHandler)
	http.HandleFunc("/outliers/onoff", outliersOnOffHandler)
	http.HandleFunc("/events", httpEvents)
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

func httpLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	var creds struct {
		Password string `json:"password"`
	}
	err := json.NewDecoder(r.Body).Decode(&creds)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	if creds.Password != CONF.password {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}
	expirationTime := time.Now().Add(15 * time.Minute)
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(expirationTime),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtSecretKey)
	if err != nil {
		http.Error(w, "Failed to create token", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    tokenString,
		Expires:  expirationTime,
		HttpOnly: true,
	})
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Login successful!"))
}

func httpCheckAuth(w http.ResponseWriter, r *http.Request) (error, int, string) {
	if CONF.password == "" {
		return nil, http.StatusOK, "Ok"
	}
	cookie, err := r.Cookie("token")
	if err != nil {
		if err == http.ErrNoCookie {
			return err, http.StatusUnauthorized, "Unauthorized"
		}
		return err, http.StatusBadRequest, "Bad request"
	}
	tokenStr := cookie.Value
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return jwtSecretKey, nil
	})
	if err != nil || !token.Valid {
		return err, http.StatusUnauthorized, "Unauthorized"
	}
	//todo: prolong token
	return nil, http.StatusOK, "Ok"
}

func httpOutliers(w http.ResponseWriter, r *http.Request) {
	err, code, msg := httpCheckAuth(w, r)
	if err != nil {
		http.Error(w, msg, code)
		return
	}
	fData, err := json.Marshal(&OTL)
	if err != nil {
		errorLog.Println(err)
		http.Error(w, "Internal error reading data", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(fData)
}

func parseDetectorID(w http.ResponseWriter, r *http.Request) (int, bool) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, `{"status":"error","message":"missing id"}`, http.StatusBadRequest)
		return 0, false
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, `{"status":"error","message":"invalid id"}`, http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func outliersUpdateHandler(w http.ResponseWriter, r *http.Request) {
	err, code, msg := httpCheckAuth(w, r)
	if err != nil {
		http.Error(w, msg, code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	id, ok := parseDetectorID(w, r)
	if !ok {
		return
	}
	d := OTL.Detectors[id]
	if d == nil {
		http.Error(w, `{"status":"error", "message":"detector not found"}`, http.StatusNotFound)
		return
	}
	go func(det *Detector) {
		infoLog.Printf("Manual outlier detection triggered for '%s'", det.Title)
		err := det.detectOutliers()
		if err != nil {
			errorLog.Printf("Error detecting outliers for '%s': %v", det.Title, err)
		}
		broadcastSSEUpdate(fmt.Sprintf(`{"status":"completed", "detector":"%d"}`, d.Id))
	}(d)

	json.NewEncoder(w).Encode(struct {
		Status   string    `json:"status"`
		Detector *Detector `json:"detector"`
	}{
		Status:   "started",
		Detector: d,
	})
}

func outliersPlotHandler(w http.ResponseWriter, r *http.Request) {
	err, code, msg := httpCheckAuth(w, r)
	if err != nil {
		http.Error(w, msg, code)
		return
	}
	id, ok := parseDetectorID(w, r)
	if !ok {
		return
	}
	var d *Detector
	if d = OTL.Detectors[id]; d == nil {
		http.Error(w, "Detector not found", http.StatusNotFound)
		return
	}

	db, err := OTL.GetDB(d.ConnectionName)
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
	err, code, msg := httpCheckAuth(w, r)
	if err != nil {
		http.Error(w, msg, code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	id, ok := parseDetectorID(w, r)
	if !ok {
		return
	}
	d := OTL.Detectors[id]
	if d == nil {
		http.Error(w, `{"status":"error", "message":"detector not found"}`, http.StatusNotFound)
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

	json.NewEncoder(w).Encode(struct {
		Status   string    `json:"status"`
		Detector *Detector `json:"detector"`
	}{
		Status:   "ok",
		Detector: d,
	})
}

func (ot *Outliers) initNotifiers() error {
	ot.Notifiers = make(map[string]Notifier)

	for _, n := range ot.parsedConf.Notifications {
		//todo: warning on overwrite
		switch n.Type {
		case "slack":
			ot.Notifiers[n.Title] = &SlackNotification{
				Title:      n.Title,
				WebhookURL: resolveEnvVar(n.WebhookURL),
			}
		case "email":
			ot.Notifiers[n.Title] = &EmailNotification{
				Title:              n.Title,
				SMTPServerWithPort: resolveEnvVar(n.SMTPServerWithPort),
				Username:           resolveEnvVar(n.Username),
				Password:           resolveEnvVar(n.Password),
				From:               resolveEnvVar(n.From),
				CommonRecipients:   n.CommonRecipients,
			}
		default:
			errorLog.Printf("Unknown notifier type '%s' for '%s'\n", n.Type, n.Title)
		}
	}

	infoLog.Printf("Loaded %d notifiers\n", len(ot.Notifiers))
	return nil
}

func (s *SlackNotification) Notify(message string, d *Detector) error {
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

func (e *EmailNotification) Notify(message string, d *Detector) error {
	if e.SMTPServerWithPort == "" {
		return fmt.Errorf("no SMTP server configured for %s", e.Title)
	}

	to := combineRecipients(e.CommonRecipients, d.NotifyEmails)
	if len(to) == 0 {
		return fmt.Errorf("no recipients defined for %s", e.Title)
	}

	host, _, err := net.SplitHostPort(e.SMTPServerWithPort)
	if err != nil {
		return fmt.Errorf("invalid smtp_server: %v", err)
	}

	msg := []byte(fmt.Sprintf(
		"To: %s\r\nSubject: Outliers in %s\r\n\r\n%s\r\n",
		strings.Join(to, ", "),
		d.Title,
		message,
	))

	auth := smtp.PlainAuth("", e.Username, e.Password, host)
	err = smtp.SendMail(e.SMTPServerWithPort, auth, e.From, to, msg)
	if err != nil {
		return fmt.Errorf("failed to send email via %s: %v", e.SMTPServerWithPort, err)
	}
	infoLog.Printf("Email sent by '%s' to %v for detector '%s'", e.Title, strings.Join(to, ", "), d.Title)
	return nil
}

func (n *SlackNotification) GetTitle() string {
	return n.Title
}

func (n *EmailNotification) GetTitle() string {
	return n.Title
}

func (ot *Outliers) initConnections() error {
	ot.CloseAllConnections()
	ot.Connections = make(map[string]Connection)

	for _, c := range ot.parsedConf.Connections {
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

		con := PostgresConnection{
			Title:           c.Title,
			ConnStr:         resolveEnvVar(c.ConnStr),
			MaxOpenConns:    c.MaxOpenConns,
			MaxIdleConns:    c.MaxIdleConns,
			ConnMaxLifetime: c.ConnMaxLifetime,
			DB:              db,
		}

		ot.Connections[c.Title] = con
		infoLog.Printf("Opened connection '%s' (type=%s)", c.Title, c.Type)
	}

	infoLog.Printf("Loaded %d connections", len(ot.Connections))
	return nil
}

func (ot *Outliers) CloseAllConnections() error {
	var firstErr error
	for k, c := range ot.Connections {
		if c != nil && c.Close() != nil {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			infoLog.Printf("Closed connection '%s'", k)
		}
		delete(ot.Connections, k)
	}
	return firstErr
}

func (ot *Outliers) GetDB(connName string) (*sql.DB, error) {
	con, ok := ot.Connections[connName]
	if !ok {
		return nil, fmt.Errorf("connection '%s' not found", connName)
	}
	return con.GetDB(), nil
}

func (c PostgresConnection) GetDB() *sql.DB {
	return c.DB
}

func (c PostgresConnection) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

func resolveEnvVar(s string) string {
	if strings.HasPrefix(s, "$") {
		val := os.Getenv(strings.TrimPrefix(s, "$"))
		if val != "" {
			return val
		}
	}
	return s
}

func combineRecipients(base, extra []string) []string {
	seen := make(map[string]bool)
	var all []string

	for _, addr := range append(base, extra...) {
		addr = strings.TrimSpace(addr)
		if addr != "" && !seen[addr] {
			seen[addr] = true
			all = append(all, addr)
		}
	}
	return all
}

type sseClients struct {
	clients map[chan string]bool
	mu      sync.Mutex
}

// todo: avoid global variables
var SSECLIENTS = &sseClients{
	clients: make(map[chan string]bool),
}

func addSSEClient(ch chan string) {
	SSECLIENTS.mu.Lock()
	defer SSECLIENTS.mu.Unlock()
	SSECLIENTS.clients[ch] = true
}

func removeSSEClient(ch chan string) {
	SSECLIENTS.mu.Lock()
	defer SSECLIENTS.mu.Unlock()
	delete(SSECLIENTS.clients, ch)
	close(ch)
}

func broadcastSSEUpdate(msg string) {
	SSECLIENTS.mu.Lock()
	defer SSECLIENTS.mu.Unlock()
	infoLog.Printf("Event '%s'", msg)
	for ch := range SSECLIENTS.clients {
		select {
		case ch <- msg:
		default: // drop message if channel overflows
		}
	}
}

func httpEvents(w http.ResponseWriter, r *http.Request) {
	err, code, msg := httpCheckAuth(w, r)
	if err != nil {
		http.Error(w, msg, code)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	clientChan := make(chan string, 30)
	addSSEClient(clientChan)
	defer removeSSEClient(clientChan)

	for msg := range clientChan {
		fmt.Fprintf(w, "data: %s\n\n", msg)
		flusher.Flush()
	}
}
