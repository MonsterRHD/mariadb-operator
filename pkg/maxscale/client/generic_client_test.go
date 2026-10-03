package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusClassification(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusConflict)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	if err := client.Server.Create(ctx, "mdb-0", &ServerAttributes{}); !IsConflict(err) {
		t.Fatalf("expected conflict error, got: %v", err)
	}
	if err := client.Server.Delete(ctx, "mdb-0"); !IsNotFound(err) {
		t.Fatalf("expected not found error, got: %v", err)
	}
}

func TestGetRawData(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"id":"mdb-0","type":"servers","attributes":{"parameters":{"address":"mdb-0"}}}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}
	data, err := client.Server.GetRawData(ctx, "mdb-0")
	if err != nil {
		t.Fatalf("error getting raw data: %v", err)
	}
	attrs, _ := data["attributes"].(map[string]any)
	params, _ := attrs["parameters"].(map[string]any)
	if address, _ := params["address"].(string); address != "mdb-0" {
		t.Fatalf("expected raw address parameter, got: %v", params)
	}
}
