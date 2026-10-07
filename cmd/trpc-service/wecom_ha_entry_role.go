package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// wecomHAEntryConfig describes the small, stable callback entrypoint used by
// the local multi-container drill. Backends are private Compose addresses; no
// node address is exposed to the callback provider.
type wecomHAEntryConfig struct {
	ListenAddress string
	Backends      []*url.URL
	ProbeInterval time.Duration
}

func loadWeComHAEntryConfig(getenv func(string) string) (wecomHAEntryConfig, error) {
	if getenv == nil {
		return wecomHAEntryConfig{}, errors.New("missing environment reader")
	}
	interval := time.Second
	if value := strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 100*time.Millisecond || parsed > time.Minute {
			return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry probe interval")
		}
		interval = parsed
	}
	parts := strings.Split(strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_BACKENDS")), ",")
	if len(parts) < 2 {
		return wecomHAEntryConfig{}, errors.New("at least two WeCom HA entry backends are required")
	}
	backends := make([]*url.URL, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		parsed, err := url.Parse(strings.TrimSpace(part))
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
			return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry backend")
		}
		if _, exists := seen[parsed.String()]; exists {
			return wecomHAEntryConfig{}, errors.New("duplicate WeCom HA entry backend")
		}
		seen[parsed.String()] = struct{}{}
		backends = append(backends, parsed)
	}
	listenAddress := valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080")
	if strings.TrimSpace(listenAddress) != listenAddress || listenAddress == "" {
		return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry listen address")
	}
	return wecomHAEntryConfig{ListenAddress: listenAddress, Backends: backends, ProbeInterval: interval}, nil
}

type wecomHAEntryBackend struct {
	URL     string `json:"url"`
	Healthy bool   `json:"healthy"`
}

type wecomHAEntry struct {
	backends []wecomHAEntryBackend
	health   []atomic.Bool
	next     atomic.Uint64
	client   *http.Client
}

func newWeComHAEntry(backends []*url.URL, client *http.Client) (*wecomHAEntry, error) {
	if len(backends) < 2 {
		return nil, errors.New("at least two backends are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	entry := &wecomHAEntry{backends: make([]wecomHAEntryBackend, len(backends)), health: make([]atomic.Bool, len(backends)), client: client}
	for index, backend := range backends {
		if backend == nil {
			return nil, errors.New("nil backend")
		}
		entry.backends[index] = wecomHAEntryBackend{URL: backend.String()}
	}
	return entry, nil
}

func (entry *wecomHAEntry) probe(ctx context.Context) {
	for index, backend := range entry.backends {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.URL+"/readyz", nil)
		if err != nil {
			entry.health[index].Store(false)
			continue
		}
		response, err := entry.client.Do(request)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
			response.Body.Close()
		}
		entry.health[index].Store(err == nil && response.StatusCode == http.StatusOK)
	}
}

func (entry *wecomHAEntry) anyHealthy() bool {
	for index := range entry.backends {
		if entry.health[index].Load() {
			return true
		}
	}
	return false
}

func (entry *wecomHAEntry) nextHealthy(excluded map[int]struct{}) (int, bool) {
	start := int(entry.next.Add(1)-1) % len(entry.backends)
	for offset := range entry.backends {
		index := (start + offset) % len(entry.backends)
		if entry.health[index].Load() {
			if _, skip := excluded[index]; !skip {
				return index, true
			}
		}
	}
	return 0, false
}

func (entry *wecomHAEntry) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/livez":
		writer.WriteHeader(http.StatusOK)
		return
	case "/readyz":
		if !entry.anyHealthy() {
			http.Error(writer, "no healthy callback backend", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		return
	case "/statusz":
		status := make([]wecomHAEntryBackend, len(entry.backends))
		copy(status, entry.backends)
		for index := range status {
			status[index].Healthy = entry.health[index].Load()
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"backends": status})
		return
	}
	if request.URL.Path != "/callbacks/wecom" {
		http.NotFound(writer, request)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 2<<20))
	if err != nil {
		http.Error(writer, "callback body too large", http.StatusRequestEntityTooLarge)
		return
	}
	excluded := make(map[int]struct{}, len(entry.backends))
	for range entry.backends {
		index, ok := entry.nextHealthy(excluded)
		if !ok {
			break
		}
		excluded[index] = struct{}{}
		response, forwardErr := entry.forward(request, body, entry.backends[index].URL)
		if forwardErr != nil {
			entry.health[index].Store(false)
			continue
		}
		defer response.Body.Close()
		copyHeaders(writer.Header(), response.Header)
		writer.WriteHeader(response.StatusCode)
		_, _ = io.Copy(writer, response.Body)
		return
	}
	http.Error(writer, "no healthy callback backend", http.StatusServiceUnavailable)
}

func (entry *wecomHAEntry) forward(inbound *http.Request, body []byte, backend string) (*http.Response, error) {
	target := backend + inbound.URL.RequestURI()
	request, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyHeaders(request.Header, inbound.Header)
	request.Host = ""
	return entry.client.Do(request)
}

func copyHeaders(destination, source http.Header) {
	for key, values := range source {
		destination.Del(key)
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func runWeComHAEntryRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	config, err := loadWeComHAEntryConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	entry, err := newWeComHAEntry(config.Backends, nil)
	if err != nil {
		return err
	}
	entry.probe(parent)
	processCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		ticker := time.NewTicker(config.ProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-processCtx.Done():
				return
			case <-ticker.C:
				entry.probe(processCtx)
			}
		}
	}()
	server := &http.Server{Addr: config.ListenAddress, Handler: http.HandlerFunc(entry.serveHTTP), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case runErr := <-errCh:
		if errors.Is(runErr, http.ErrServerClosed) {
			return nil
		}
		return runErr
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
