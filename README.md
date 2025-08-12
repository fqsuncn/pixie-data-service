# Pixie Data Service

This project aims to fetch data by calling the Pixie API. The Pixie cluster is installed locally.

## Configuration

The application requires the following configuration in `config.json`:
- `px_api_key`: Your Pixie API key
- `px_cluster_id`: Your Pixie cluster ID
- `cloud_addr`: Pixie cloud address (default: dev.withpixie.dev:443)

## Running the Service

To run the service, execute:
```
go run main.go
```

## API Usage Example

Once the service is running, you can call the API using curl:
```bash
curl http://localhost:8080/pixie
```

This will return a JSON response containing the query results from Pixie, including columns and rows of data.

Example response format:
```json
{
  "columns": ["upid", "req_path", "remote_addr", "req_method"],
  "rows": [
    ["12345", "/api/users", "192.168.1.100", "GET"],
    ["67890", "/login", "192.168.1.101", "POST"]
  ],
  "stats": {
    "execution_time_ms": 123,
    "row_count": 2
  }
}
```