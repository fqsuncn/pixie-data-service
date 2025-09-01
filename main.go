package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"time"

	"px.dev/pxapi"
	"px.dev/pxapi/types"
)

// readPXLScript reads the PXL script from a file
func readPXLScript(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("could not read PXL script file: %w", err)
	}
	return string(data), nil
}

// Config holds application configuration
type Config struct {
	PXAPIKey    string `json:"px_api_key"`
	PXClusterID string `json:"px_cluster_id"`
	CloudAddr   string `json:"cloud_addr"`
}

// loadConfig reads configuration from a JSON file
func loadConfig(filename string) (*Config, error) {
	// Read config file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	// Parse JSON
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("could not parse config file: %w", err)
	}

	// Validate required fields
	if config.PXAPIKey == "" {
		return nil, fmt.Errorf("PX_API_KEY is not set in config file")
	}
	if config.PXClusterID == "" {
		return nil, fmt.Errorf("PX_CLUSTER_ID is not set in config file")
	}
	if config.CloudAddr == "" {
		return nil, fmt.Errorf("CLOUD_ADDR is not set in config file")
	}

	return &config, nil
}

// tablePrinter accumulates query results
type tablePrinter struct {
	cols []string
	rows [][]string
}

// Implement TableMuxer interface
func (t *tablePrinter) AcceptTable(ctx context.Context, metadata types.TableMetadata) (pxapi.TableRecordHandler, error) {
	// Initialize column names here since we have access to metadata
	v := reflect.ValueOf(metadata)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	// Try to find column names in common field names
	fieldNames := []string{"Columns", "ColNames", "Fields", "Schema"}
	for _, name := range fieldNames {
		field := v.FieldByName(name)
		if field.IsValid() {
			// Handle different possible types for column names
			if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.String {
				t.cols = field.Interface().([]string)
				break
			} else if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Struct {
				// If it's a slice of structs, try to get Name field from each
				for i := 0; i < field.Len(); i++ {
					item := field.Index(i)
					nameField := item.FieldByName("Name")
					if nameField.IsValid() && nameField.Kind() == reflect.String {
						t.cols = append(t.cols, nameField.String())
					}
				}
				if len(t.cols) > 0 {
					break
				}
			}
		}
	}
	return t, nil
}

// Implement TableRecordHandler interface
func (t *tablePrinter) HandleInit(ctx context.Context, metadata types.TableMetadata) error {
	// Column names are initialized in AcceptTable method
	return nil
}

func (t *tablePrinter) HandleRecord(ctx context.Context, r *types.Record) error {
	var row []string
	for _, d := range r.Data {
		row = append(row, d.String())
	}
	t.rows = append(t.rows, row)
	return nil
}

func (t *tablePrinter) HandleDone(ctx context.Context) error {
	return nil
}

// pixieScriptHandler handles requests to execute a script from the scripts directory
func pixieScriptHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("INFO: Received script file request from %s", r.RemoteAddr)
	
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get script name from URL path
	// Extract the script name after "/pixie/script/"
	scriptName := r.URL.Path[len("/pixie/script/"):]
	if scriptName == "" {
		http.Error(w, "Script name is required", http.StatusBadRequest)
		return
	}

	// Read script from the scripts directory
	scriptPath := fmt.Sprintf("scripts/%s", scriptName)
	scriptContent, err := readPXLScript(scriptPath)
	if err != nil {
		log.Printf("ERROR: Failed to read script file %s: %v", scriptPath, err)
		http.Error(w, "Failed to read script file: " + err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Starting query with script file %s", scriptName)

	// Load config
	config, err := loadConfig("config.json")
	if err != nil {
		log.Printf("ERROR: Failed to load config: %v\n", err)
		http.Error(w, "Failed to load configuration", http.StatusInternalServerError)
		return
	}

	// Create Pixie client
	ctx := context.Background()
	client, err := pxapi.NewClient(
		ctx,
		pxapi.WithAPIKey(config.PXAPIKey),
		pxapi.WithCloudAddr(config.CloudAddr),
		pxapi.WithE2EEncryption(true),
	)
	if err != nil {
		http.Error(w, "Failed to create Pixie API client: " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Connect to Vizier
	vizCtx, vizCancel := context.WithTimeout(ctx, 30*time.Second)
	defer vizCancel()
	vz, err := client.NewVizierClient(vizCtx, config.PXClusterID)
	if err != nil {
		http.Error(w, "Failed to connect to cluster: " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute script
	tp := &tablePrinter{}
	execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
	defer execCancel()
	rs, err := vz.ExecuteScript(execCtx, scriptContent, tp)
	if err != nil {
		http.Error(w, "Script execution failed: " + err.Error(), http.StatusBadRequest)
		return
	}
	defer rs.Close()

	if err := rs.Stream(); err != nil {
		http.Error(w, "Streaming failed: " + err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("INFO: Query completed successfully for script %s, returned %d rows and %d columns", scriptName, len(tp.rows), len(tp.cols))

	// Return JSON
	output := map[string]interface{}{
		"columns": tp.cols,
		"rows":    tp.rows,
		"stats":   rs.Stats(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(output); err != nil {
		log.Printf("ERROR: Failed to encode response: %v", err)
	} else {
		log.Printf("INFO: Response returned to %s", r.RemoteAddr)
	}
}

func pixieHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("INFO: Received request from %s", r.RemoteAddr)
	
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req struct {
		Script string `json:"script"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Script == "" {
		http.Error(w, "Missing 'script' field in request body", http.StatusBadRequest)
		return
	}

	log.Printf("INFO: Starting query with script length %d characters", len(req.Script))

	// Load config
	config, err := loadConfig("config.json")
	if err != nil {
		log.Printf("ERROR: Failed to load config: %v\n", err)
		http.Error(w, "Failed to load configuration", http.StatusInternalServerError)
		return
	}

	// Create Pixie client
	ctx := context.Background()
	client, err := pxapi.NewClient(
		ctx,
		pxapi.WithAPIKey(config.PXAPIKey),
		pxapi.WithCloudAddr(config.CloudAddr),
		pxapi.WithE2EEncryption(true),
	)
	if err != nil {
		http.Error(w, "Failed to create Pixie API client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Connect to Vizier
	vizCtx, vizCancel := context.WithTimeout(ctx, 30*time.Second)
	defer vizCancel()
	vz, err := client.NewVizierClient(vizCtx, config.PXClusterID)
	if err != nil {
		http.Error(w, "Failed to connect to cluster: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute script
	tp := &tablePrinter{}
	execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
	defer execCancel()
	rs, err := vz.ExecuteScript(execCtx, req.Script, tp)
	if err != nil {
		http.Error(w, "Script execution failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer rs.Close()

	if err := rs.Stream(); err != nil {
		http.Error(w, "Streaming failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("INFO: Query completed successfully, returned %d rows and %d columns", len(tp.rows), len(tp.cols))

	// Return JSON
	output := map[string]interface{}{
		"columns": tp.cols,
		"rows":    tp.rows,
		"stats":   rs.Stats(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(output); err != nil {
		log.Printf("ERROR: Failed to encode response: %v", err)
	} else {
		log.Printf("INFO: Response returned to %s", r.RemoteAddr)
	}
}

// ServeOpenAPI serves the OpenAPI specification file
func ServeOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, "openapi.json")
}

func main() {
	// 设置日志功能：同时输出到控制台和logs目录下的文件
	// 创建logs目录（如果不存在）
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("ERROR: Failed to create log directory: %v\n", err)
	}

	// 创建日志文件 - 每次重启生成新文件（包含时间戳）
	logFileName := fmt.Sprintf("%s/app-%s-%s.log", logDir, time.Now().Format("2006-01-02"), time.Now().Format("15-04-05"))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("ERROR: Failed to open log file: %v\n", err)
	} else {
		// 设置日志同时输出到控制台和文件
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	}

	http.HandleFunc("/pixie", pixieHandler)
	http.HandleFunc("/pixie/script/", pixieScriptHandler)
	http.HandleFunc("/openapi.json", ServeOpenAPI)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	log.Println("Server running on :8080")
	log.Println("OpenAPI specification available at http://localhost:8080/openapi.json")
	log.Println("Swagger UI available at http://localhost:8080/")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
