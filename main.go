package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
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

	//支持跨域访问start
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	log.Printf("INFO: Received script file request from %s", r.RemoteAddr)

	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	//支持跨域访问end

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



	// Read script from the scripts directory - automatically add .pxl extension
	scriptPath := fmt.Sprintf("scripts/%s.pxl", scriptName)
	scriptContent, err := readPXLScript(scriptPath)
	if err != nil {
		log.Printf("ERROR: Failed to read script file %s: %v", scriptPath, err)
		http.Error(w, "Failed to read script file: "+err.Error(), http.StatusNotFound)
		return
	}

	// Parse macros from request header
	macrosHeader := r.Header.Get("macros")
	if macrosHeader != "" {
		var macros map[string]string
		if err := json.Unmarshal([]byte(macrosHeader), &macros); err != nil {
			log.Printf("WARN: Invalid macros header format: %v", err)
		} else {
			// Replace all macro placeholders from the macros map
			for key, value := range macros {
				placeholder := fmt.Sprintf("{%s}", key)
				scriptContent = strings.ReplaceAll(scriptContent, placeholder, value)
				log.Printf("INFO: Replaced macro %s with value %s", placeholder, value)
			}
		}
	}

	log.Printf("INFO: Starting query with script file %s", scriptName)
	log.Printf("INFO: Executing script content: %s", scriptContent)

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
	rs, err := vz.ExecuteScript(execCtx, scriptContent, tp)
	if err != nil {
		http.Error(w, "Script execution failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer rs.Close()

	if err := rs.Stream(); err != nil {
		http.Error(w, "Streaming failed: "+err.Error(), http.StatusInternalServerError)
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

// gzipHandler 包装HTTP处理器，为支持gzip的客户端提供压缩响应
func gzipHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 检查客户端是否支持gzip压缩
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			// 如果不支持，直接调用原始处理器
			handler(w, r)
			return
		}

		// 设置响应头，表明内容已被gzip压缩
		w.Header().Set("Content-Encoding", "gzip")

		// 创建一个gzip响应写入器
		gz := gzip.NewWriter(w)
		defer gz.Close()

		// 创建一个包装了gzip写入器的响应写入器
		gzw := &gzipResponseWriter{
			ResponseWriter: w,
			writer:         gz,
		}

		// 调用原始处理器，但传入包装后的响应写入器
		handler(gzw, r)
	}
}

// gzipResponseWriter 包装http.ResponseWriter，将写入的数据压缩后输出
// 实现http.ResponseWriter接口
// 实现io.Writer接口
// 实现http.Flusher接口，确保流式响应正常工作
type gzipResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

// Write 重写http.ResponseWriter的Write方法，将数据写入gzip写入器
func (grw *gzipResponseWriter) Write(b []byte) (int, error) {
	// 确保Content-Type已设置
	if len(grw.Header().Get("Content-Type")) == 0 {
		grw.Header().Set("Content-Type", http.DetectContentType(b))
	}
	return grw.writer.Write(b)
}

// WriteHeader 重写http.ResponseWriter的WriteHeader方法
// 注意：由于使用了gzip压缩，Content-Length会被忽略，所以这里不需要删除Content-Length头
func (grw *gzipResponseWriter) WriteHeader(statusCode int) {
	grw.ResponseWriter.WriteHeader(statusCode)
}

// Flush 实现http.Flusher接口，确保支持流式响应
func (grw *gzipResponseWriter) Flush() {
	if flusher, ok := grw.ResponseWriter.(http.Flusher); ok {
		// 先刷新gzip写入器，确保所有数据都已压缩并写入底层写入器
		grw.writer.(*gzip.Writer).Flush()
		// 然后刷新底层的Flusher
		flusher.Flush()
	}
}

func main() {
	// 设置日志只输出到stdout
	log.SetOutput(os.Stdout)

	// 使用gzip处理器包装所有HTTP处理器
	http.HandleFunc("/pixie", gzipHandler(pixieHandler))
	http.HandleFunc("/pixie/script/", gzipHandler(pixieScriptHandler))
	http.HandleFunc("/openapi.json", gzipHandler(ServeOpenAPI))
	http.HandleFunc("/", gzipHandler(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	}))
	log.Println("Server running on :8080")
	log.Println("OpenAPI specification available at http://localhost:8080/openapi.json")
	log.Println("Swagger UI available at http://localhost:8080/")
	log.Println("Response compression (gzip) enabled for supporting clients")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
