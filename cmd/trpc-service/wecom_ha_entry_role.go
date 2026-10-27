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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// wecomHAEntryConfig describes the small, stable callback entrypoint used by
// the local multi-container drill. Backends are private Compose addresses; no
// node address is exposed to the callback provider.
type wecomHAEntryConfig struct {
	ListenAddress     string
	Backends          []*url.URL
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	RequestTimeout    time.Duration
	HealthTTL         time.Duration
	RecoverySuccesses int
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
	config := wecomHAEntryConfig{ListenAddress: listenAddress, Backends: backends, ProbeInterval: interval, ProbeTimeout: time.Second, RequestTimeout: 2 * time.Second, RecoverySuccesses: 2}
	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{"TRPC_WECOM_HA_ENTRY_PROBE_TIMEOUT", &config.ProbeTimeout}, {"TRPC_WECOM_HA_ENTRY_REQUEST_TIMEOUT", &config.RequestTimeout},
	} {
		if raw := strings.TrimSpace(getenv(setting.name)); raw != "" {
			duration, err := time.ParseDuration(raw)
			if err != nil || duration < 100*time.Millisecond || duration > 30*time.Second {
				return config, errors.New("invalid WeCom HA entry timeout")
			}
			*setting.target = duration
		}
	}
	config.HealthTTL = 3 * (interval + config.ProbeTimeout)
	if raw := strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_RECOVERY_SUCCESSES")); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count < 1 || count > 10 {
			return config, errors.New("invalid entry recovery threshold")
		}
		config.RecoverySuccesses = count
	}
	return config, nil
}

type wecomHAEntryBackend struct {
	URL                  string    `json:"url"`
	Healthy              bool      `json:"healthy"`
	CheckedAt            time.Time `json:"checked_at,omitempty"`
	HealthySince         time.Time `json:"healthy_since,omitempty"`
	Failures             uint64    `json:"failures"`
	LastFailure          string    `json:"last_failure,omitempty"`
	ConsecutiveSuccesses int       `json:"consecutive_successes"`
}

type wecomHAEntry struct {
	backends                                []wecomHAEntryBackend
	health                                  []atomic.Bool
	next                                    atomic.Uint64
	client                                  *http.Client
	mu                                      sync.Mutex
	epoch                                   []uint64
	probeTimeout, requestTimeout, healthTTL time.Duration
	recoverySuccesses                       int
}

func newWeComHAEntry(backends []*url.URL, client *http.Client) (*wecomHAEntry, error) {
	if len(backends) < 2 {
		return nil, errors.New("at least two backends are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	entry := &wecomHAEntry{backends: make([]wecomHAEntryBackend, len(backends)), health: make([]atomic.Bool, len(backends)), client: client}
	entry.client = &clientCopy
	entry.epoch = make([]uint64, len(backends))
	entry.probeTimeout, entry.requestTimeout, entry.healthTTL, entry.recoverySuccesses = time.Second, 2*time.Second, 6*time.Second, 2
	for index, backend := range backends {
		if backend == nil {
			return nil, errors.New("nil backend")
		}
		entry.backends[index] = wecomHAEntryBackend{URL: backend.String()}
	}
	return entry, nil
}

func (entry *wecomHAEntry) probe(ctx context.Context) {
	var pending sync.WaitGroup
	for index := range entry.backends {
		pending.Add(1)
		go func(index int) {
			defer pending.Done()
			entry.mu.Lock()
			epoch := entry.epoch[index]
			entry.mu.Unlock()
			probeCtx, cancel := context.WithTimeout(ctx, entry.probeTimeout)
			defer cancel()
			request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, entry.backends[index].URL+"/readyz", nil)
			if err != nil {
				entry.recordProbe(index, epoch, false)
				return
			}
			response, err := entry.client.Do(request)
			if err == nil && response != nil && response.Body != nil {
				_, bodyErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
				if bodyErr != nil {
					err = bodyErr
				}
				response.Body.Close()
			}
			entry.recordProbe(index, epoch, err == nil && probeCtx.Err() == nil && response != nil && response.Body != nil && response.StatusCode == http.StatusOK)
		}(index)
	}
	pending.Wait()
}

func (entry *wecomHAEntry) recordProbe(index int, epoch uint64, healthy bool) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.epoch[index] != epoch {
		return
	}
	backend := &entry.backends[index]
	if !backend.CheckedAt.IsZero() && time.Since(backend.CheckedAt) > entry.healthTTL {
		backend.Failures++
		backend.LastFailure = "health_expired"
		backend.ConsecutiveSuccesses = 0
		backend.HealthySince = time.Time{}
		entry.health[index].Store(false)
	}
	backend.CheckedAt = time.Now().UTC()
	if !healthy {
		entry.epoch[index]++
		backend.Failures++
		backend.LastFailure = "probe_failed"
		backend.ConsecutiveSuccesses = 0
		backend.HealthySince = time.Time{}
		entry.health[index].Store(false)
		return
	}
	backend.ConsecutiveSuccesses++
	if backend.Failures == 0 || backend.ConsecutiveSuccesses >= entry.recoverySuccesses {
		if !entry.health[index].Load() {
			backend.HealthySince = time.Now().UTC()
		}
		entry.health[index].Store(true)
		backend.LastFailure = ""
	}
}
func (entry *wecomHAEntry) failBackend(index int, reason string) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.epoch[index]++
	backend := &entry.backends[index]
	backend.Failures++
	backend.LastFailure = reason
	backend.ConsecutiveSuccesses = 0
	backend.HealthySince = time.Time{}
	entry.health[index].Store(false)
}
func (entry *wecomHAEntry) healthy(index int) bool {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.health[index].Load() && !entry.backends[index].CheckedAt.IsZero() && time.Since(entry.backends[index].CheckedAt) <= entry.healthTTL
}

func (entry *wecomHAEntry) anyHealthy() bool {
	for index := range entry.backends {
		if entry.healthy(index) {
			return true
		}
	}
	return false
}

func (entry *wecomHAEntry) nextHealthy(excluded map[int]struct{}) (int, bool) {
	start := int((entry.next.Add(1) - 1) % uint64(len(entry.backends)))
	for offset := range entry.backends {
		index := (start + offset) % len(entry.backends)
		if entry.healthy(index) {
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
		entry.mu.Lock()
		status := make([]wecomHAEntryBackend, len(entry.backends))
		copy(status, entry.backends)
		for index := range status {
			status[index].Healthy = entry.health[index].Load() && !status[index].CheckedAt.IsZero() && time.Since(status[index].CheckedAt) <= entry.healthTTL
		}
		entry.mu.Unlock()
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
		forwardCtx, cancel := context.WithTimeout(request.Context(), entry.requestTimeout)
		response, forwardErr := entry.forward(request.WithContext(forwardCtx), body, entry.backends[index].URL)
		if forwardErr != nil {
			cancel()
			if request.Context().Err() != nil {
				return
			}
			entry.failBackend(index, "transport_failed")
			continue
		}
		if response == nil || response.Body == nil {
			cancel()
			entry.failBackend(index, "invalid_response")
			continue
		}
		if response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout {
			response.Body.Close()
			cancel()
			entry.failBackend(index, "backend_unavailable")
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
		response.Body.Close()
		cancel()
		if request.Context().Err() != nil {
			return
		}
		if readErr != nil || len(responseBody) > 2<<20 {
			if response.StatusCode >= 400 && response.StatusCode < 500 {
				http.Error(writer, "callback rejected", response.StatusCode)
				return
			}
			entry.failBackend(index, "response_incomplete")
			continue
		}
		copyHeaders(writer.Header(), response.Header)
		writer.WriteHeader(response.StatusCode)
		_, _ = writer.Write(responseBody)
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
	hop := map[string]bool{"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true}
	for _, token := range strings.Split(source.Get("Connection"), ",") {
		hop[http.CanonicalHeaderKey(strings.TrimSpace(token))] = true
	}
	for key, values := range source {
		if hop[http.CanonicalHeaderKey(key)] {
			continue
		}
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
	entry.probeTimeout, entry.requestTimeout, entry.healthTTL, entry.recoverySuccesses = config.ProbeTimeout, config.RequestTimeout, config.HealthTTL, config.RecoverySuccesses
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
