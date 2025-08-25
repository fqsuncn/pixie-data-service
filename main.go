package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"px.dev/pxapi"
	"px.dev/pxapi/errdefs"
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

func pixieHandler(w http.ResponseWriter, r *http.Request) {
	// Read configuration from config.json
	config, err := loadConfig("config.json")
	if err != nil {
		log.Printf("ERROR: Failed to load config: %v\n", err)
		http.Error(w, "Failed to load configuration", http.StatusInternalServerError)
		return
	}

	// Get API credentials from config
	apiKey := config.PXAPIKey
	clusterID := config.PXClusterID

	// Log that we're checking credentials (without exposing them)
	log.Println("Checking Pixie API credentials...")

	// Validate credentials
	if apiKey == "" {
		log.Println("ERROR: PX_API_KEY environment variable is not set")
		http.Error(w, "PX_API_KEY environment variable is not set", http.StatusInternalServerError)
		return
	}
	if clusterID == "" {
		log.Println("ERROR: PX_CLUSTER_ID environment variable is not set")
		http.Error(w, "PX_CLUSTER_ID environment variable is not set", http.StatusInternalServerError)
		return
	}

	// Log credential details (without exposing sensitive information)
	log.Printf("API key provided (length: %d)", len(apiKey))
	log.Printf("Cluster ID provided: %s", clusterID)

	// Check if we're using the correct API version
	log.Println("Using pxapi version: v0.4.1")
	log.Println("Creating Pixie API client with the provided credentials...")

	ctx := context.Background()
	log.Println("Creating Pixie API client...")
	// Create Pixie API client with detailed error handling
	client, err := pxapi.NewClient(
		ctx,
		pxapi.WithAPIKey(apiKey),
		pxapi.WithCloudAddr(config.CloudAddr),
		pxapi.WithE2EEncryption(true),
	)
	if err != nil {
		log.Printf("ERROR creating Pixie API client: %v\n", err)
		// Log the error and return a generic message
		log.Printf("ERROR creating Pixie API client: %v\n", err)
		// Check if error message contains authentication-related keywords
		if strings.Contains(err.Error(), "unauthenticated") || strings.Contains(err.Error(), "invalid API key") {
			log.Println("ERROR: Authentication failed - invalid API key?")
			http.Error(w, "Authentication failed: Invalid API key", http.StatusUnauthorized)
		} else {
			http.Error(w, "Failed to create Pixie API client: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	log.Println("Pixie API client created successfully")
	log.Println("Attempting to connect to Vizier cluster...")

	log.Printf("Creating Vizier client for cluster: %s\n", clusterID)
	// Add timeout to Vizier client creation (increased from 30s to 60s due to 504 Gateway Timeout errors)
	vizCtx, vizCancel := context.WithTimeout(ctx, 60*time.Second)
	defer vizCancel()

	// Attempt to create Vizier client with detailed error handling
	log.Println("Creating Vizier client - this operation will fetch authentication token")
	startTime := time.Now()
	vz, err := client.NewVizierClient(vizCtx, clusterID)
	elapsed := time.Since(startTime)
	log.Printf("Vizier client creation took %v\n", elapsed)
	if err != nil {
		log.Printf("ERROR creating Vizier client: %v\n", err)
		// Check error type based on message content
		errMsg := err.Error()
		if strings.Contains(errMsg, "unauthenticated") || strings.Contains(errMsg, "invalid API key") {
			log.Printf("ERROR: Authentication failed when connecting to cluster: %v\n", err)
			log.Println("Possible causes: invalid API key, invalid cluster ID, or expired credentials")
			// Log API key length and cluster ID (without exposing sensitive info)
			log.Printf("API key length: %d, Cluster ID: %s\n", len(apiKey), clusterID)
			http.Error(w, "Authentication failed: Invalid API key or cluster ID", http.StatusUnauthorized)
		} else if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") {
			log.Printf("ERROR: Cluster %s not found", clusterID)
			http.Error(w, "Cluster not found: "+clusterID, http.StatusNotFound)
		} else if vizCtx.Err() == context.DeadlineExceeded {
			log.Println("ERROR: Timeout connecting to cluster")
			http.Error(w, "Timeout connecting to cluster", http.StatusGatewayTimeout)
		} else {
			http.Error(w, "Failed to connect to cluster: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	log.Printf("Successfully connected to Vizier cluster: %s\n", clusterID)
	log.Println("Authentication with Pixie API successful")
	log.Println("Proceeding to execute PxL script...")

	// Read PXL script from file
	pxlScript, err := readPXLScript("conn_status.pxl")
	if err != nil {
		log.Printf("ERROR: Failed to read PXL script: %v\n", err)
		http.Error(w, "Failed to read PXL script", http.StatusInternalServerError)
		return
	}

	tp := &tablePrinter{}
	rs, err := vz.ExecuteScript(ctx, pxlScript, tp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rs.Close()
	log.Println("Result set created successfully, will close when handler exits")

	log.Println("Attempting to stream results from PxL script...")
	if err := rs.Stream(); err != nil {
		log.Printf("ERROR streaming results: %v\n", err)
		if errdefs.IsCompilationError(err) {
			http.Error(w, "PxL compilation error: "+err.Error(), http.StatusBadRequest)
		} else if strings.Contains(err.Error(), "unauthenticated") || strings.Contains(err.Error(), "invalid token") {
			log.Println("ERROR: Authentication failed during script execution - invalid or expired token?")
			http.Error(w, "Authentication failed during script execution: Invalid or expired token", http.StatusUnauthorized)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	log.Println("Successfully streamed results from PxL script")

	output := map[string]interface{}{
		"columns": tp.cols,
		"rows":    tp.rows,
		"stats":   rs.Stats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(output)
}

// ServeOpenAPI serves the OpenAPI specification file
func ServeOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, "openapi.json")
}

func main() {
	http.HandleFunc("/pixie", pixieHandler)
	http.HandleFunc("/openapi.json", ServeOpenAPI)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	log.Println("Server running on :8080")
	log.Println("OpenAPI specification available at http://localhost:8080/openapi.json")
	log.Println("Swagger UI available at http://localhost:8080/")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
