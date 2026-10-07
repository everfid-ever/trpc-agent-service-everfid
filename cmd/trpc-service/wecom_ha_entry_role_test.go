package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestLoadWeComHAEntryConfig(t *testing.T) {
	values := map[string]string{
		"TRPC_WECOM_HA_ENTRY_BACKENDS":       "http://node-a:8080,http://node-b:8080",
		"TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL": "250ms",
	}
	config, err := loadWeComHAEntryConfig(mapEnvironment(values))
	if err != nil || config.ListenAddress != ":8080" || len(config.Backends) != 2 || config.ProbeInterval != 250*time.Millisecond {
		t.Fatalf("config=%+v err=%v", config, err)
	}
	for name, value := range map[string]string{
		"TRPC_WECOM_HA_ENTRY_BACKENDS":       "http://node-a:8080",
		"TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL": "10ms",
	} {
		candidate := cloneEnvironment(values)
		candidate[name] = value
		if _, err := loadWeComHAEntryConfig(mapEnvironment(candidate)); err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
	}
}

func TestWeComHAEntryProbesAndFailsOverCallbacks(t *testing.T) {
	requestsA := 0
	backendA := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/readyz" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		requestsA++
		http.Error(writer, "unexpected first backend", http.StatusBadGateway)
	}))
	defer backendA.Close()
	requestsB := 0
	backendB := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/readyz" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		requestsB++
		body, _ := io.ReadAll(request.Body)
		if request.URL.Path != "/callbacks/wecom" || string(body) != "<xml/>" || request.Header.Get("X-Callback") != "signed" {
			http.Error(writer, "invalid forwarded callback", http.StatusBadRequest)
			return
		}
		writer.Header().Set("X-Backend", "b")
		_, _ = writer.Write([]byte("accepted"))
	}))
	defer backendB.Close()
	urlA, _ := url.Parse(backendA.URL)
	urlB, _ := url.Parse(backendB.URL)
	entry, err := newWeComHAEntry([]*url.URL{urlA, urlB}, backendA.Client())
	if err != nil {
		t.Fatal(err)
	}
	entry.probe(context.Background())
	// Make the first selected backend fail at transport level, which must cause
	// one retry on the other healthy backend. Closing it after probing models a
	// node that disappeared between health checks and callback arrival.
	backendA.Close()
	request := httptest.NewRequest(http.MethodPost, "/callbacks/wecom?msg_signature=abc", strings.NewReader("<xml/>"))
	request.Header.Set("X-Callback", "signed")
	response := httptest.NewRecorder()
	entry.serveHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "accepted" || response.Header().Get("X-Backend") != "b" {
		t.Fatalf("status=%d body=%q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	if requestsA != 0 || requestsB != 1 || entry.health[0].Load() || !entry.health[1].Load() {
		t.Fatalf("requests a=%d b=%d health a=%t b=%t", requestsA, requestsB, entry.health[0].Load(), entry.health[1].Load())
	}
	status := httptest.NewRecorder()
	entry.serveHTTP(status, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"healthy":false`) || !strings.Contains(status.Body.String(), `"healthy":true`) {
		t.Fatalf("status=%d body=%s", status.Code, status.Body.String())
	}
}
