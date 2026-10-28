# 单主机多容器实例可用性 — 实现代码附录

本篇代码从当前工作区源码逐字提取；省略的文件 import/其他函数以明确的节选说明表示。每段均给出状态变化与故障含义。示例身份、测试时延与状态不是运行结果。

## 当前入口完整实现

来源：`cmd/trpc-service/wecom_ha_entry_role.go`；并行探测、健康有效期、epoch、连续成功重加入、响应缓冲与错误切换均包含在实际实现中。

```go
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
```

## 排空准入门

来源：`trpcservice/reliability/drain/gate.go`；Begin 后关闭新准入，活动任务 leave 幂等；所有已接纳项退出才关闭 Done。

```go
// Package drain separates stopping admission from canceling active work.
package drain

import "sync"

type Gate struct {
	mu           sync.Mutex
	stopping     bool
	active       int
	signal, done chan struct{}
}

func New() *Gate                        { return &Gate{signal: make(chan struct{}), done: make(chan struct{})} }
func (g *Gate) Signal() <-chan struct{} { return g.signal }
func (g *Gate) Done() <-chan struct{}   { return g.done }
func (g *Gate) Draining() bool          { g.mu.Lock(); defer g.mu.Unlock(); return g.stopping }
func (g *Gate) Begin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return
	}
	g.stopping = true
	close(g.signal)
	if g.active == 0 {
		close(g.done)
	}
}
func (g *Gate) Enter() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return nil, false
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.active--
			if g.stopping && g.active == 0 {
				close(g.done)
			}
		})
	}, true
}

func Signaled(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}
```

## 运行时停止新轮询与防重启

来源：`cmd/trpc-service/runtime_drain.go`；drainPoll 防新批次；runUntilDrained 不复活已正常排空的消费者；业务 HTTP 返回 503。

```go
package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
)

var errRuntimeDrained = errors.New("runtime admissions stopped")

func drainPoll(gate *drain.Gate, operation func(context.Context, int) (int, error)) func(context.Context, int) (int, error) {
	return func(ctx context.Context, limit int) (int, error) {
		leave, ok := gate.Enter()
		if !ok {
			return 0, errRuntimeDrained
		}
		defer leave()
		return operation(ctx, limit)
	}
}

// Keep retrying failed consumers while serving. A completed drain is final:
// do not resurrect the consumer after it has stopped admitting work.
func runUntilDrained(ctx context.Context, signal <-chan struct{}, name string, operation func(context.Context) error, logger *roleLogger) error {
	retryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := runRecoverableLoop(retryCtx, name, func(runCtx context.Context) error {
		err := operation(runCtx)
		if drain.Signaled(signal) {
			cancel()
		}
		return err
	}, 250*time.Millisecond, logger)
	if ctx.Err() == nil && drain.Signaled(signal) {
		return nil
	}
	return err
}

func drainingHTTP(next http.Handler, gate *drain.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gate.Draining() && r.URL.Path != "/livez" && r.URL.Path != "/readyz" && r.URL.Path != "/statusz" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "instance draining", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
```

## 只读校验与原子发布

来源：`cmd/trpc-service/runtime_files.go`；读节点不写；临时文件在摘要目录外，原子 link 不覆盖已有版本。

```go
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func ensureSkillDirectory(path string, readOnly bool) error {
	if readOnly {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("shared skill directory unavailable: %w", err)
		}
		if !info.IsDir() {
			return errors.New("shared skill path is not a directory")
		}
		return nil
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

// Only the bootstrap writer may publish immutable package content. Publication
// is atomic and never overwrites an existing version, even across processes.
func ensureSkillFile(stagingRoot, path string, content []byte, readOnly bool) error {
	check := func() error {
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, content) {
			return errors.New("published skill content does not match this binary")
		}
		return nil
	}
	if err := check(); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if readOnly {
		return errors.New("shared skill package is missing; run bootstrap before starting readers")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Keep unfinished files outside every published package's digest root.
	publishing := filepath.Join(stagingRoot, ".publishing")
	if err := os.MkdirAll(publishing, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(publishing, ".skill-publish-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(content); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return check()
}
```

## 组合根实际接线：排空与退出

来源：cmd/trpc-service/webui_local_role.go 的运行函数尾段（节选，依赖前文已构造的组件与drainGate）。HTTP与进度发布不参与draining WaitGroup，活动工作等待结束/到期后才取消processCtx，随后有界等待后台退出。

```go
signalCtx, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	processCtx, stop := context.WithCancel(context.WithoutCancel(parent))
	defer stop()
	server := &http.Server{Addr: configValue.ListenAddress, Handler: drainingHTTP(mux, drainGate), ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return processCtx },
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	errorsCh := make(chan error, 8)
	var background sync.WaitGroup
	var draining sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		mustDrain := name != "http" && name != "progress publisher"
		if mustDrain {
			draining.Add(1)
		}
		go func() {
			defer background.Done()
			if mustDrain {
				defer draining.Done()
			}
			operationErr := operation(processCtx)
			if operationErr == nil && !drainGate.Draining() && processCtx.Err() == nil {
				operationErr = errors.New("component stopped unexpectedly")
			}
			if operationErr != nil && !errors.Is(operationErr, context.Canceled) && !drainGate.Draining() {
				select {
				case errorsCh <- fmt.Errorf("%s stopped: %w", name, operationErr):
				case <-processCtx.Done():
				}
			}
		}()
	}
	start("preprocess", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, drainPoll(drainGate, health.poll("preprocess", func(ctx context.Context) (int, error) { return preprocessor.RunOnce(ctx, 100) })), 100*time.Millisecond, 100, logger)
	})
	start("progress publisher", progressPublisher.Run)
	start("confirmation expiry reconciler", func(ctx context.Context) error {
		reconciler := governance.ConfirmationExpiryReconciler{Coordinator: governanceStore, BatchSize: 100}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-drainGate.Signal():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				leave, ok := drainGate.Enter()
				if !ok {
					return nil
				}
				if _, expiryErr := reconciler.RunOnce(ctx); expiryErr != nil {
					health.observe("confirmation-expiry", expiryErr)
					logger.Printf("webui confirmation expiry degraded: %v", expiryErr)
				} else {
					health.observe("confirmation-expiry", nil)
				}
				leave()
			}
		}
	})
	start("dispatch relay", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, drainPoll(drainGate, health.poll("dispatch-relay", dispatchRelay.RunOnce)), 100*time.Millisecond, 100, logger)
	})
	start("worker", func(ctx context.Context) error {
		return runUntilDrained(ctx, drainGate.Signal(), "webui worker", health.run("worker", workerConsumer.Run), logger)
	})
	start("reply relay", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, drainPoll(drainGate, health.poll("reply-relay", replyRelay.RunOnce)), 100*time.Millisecond, 100, logger)
	})
	start("wakeup relay", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, drainPoll(drainGate, health.poll("wakeup-relay", wakeupRelay.RunOnce)), 100*time.Millisecond, 100, logger)
	})
	start("wakeup dispatcher", func(ctx context.Context) error {
		return runUntilDrained(ctx, drainGate.Signal(), "webui wakeup dispatcher", health.run("wakeup", wakeupDispatcher.Run), logger)
	})
	start("delivery", func(ctx context.Context) error {
		return runUntilDrained(ctx, drainGate.Signal(), "webui delivery", health.run("delivery", deliverySupervisor.Run), logger)
	})
	start("http", func(context.Context) error { return server.ListenAndServe() })
	logger.Printf("WebUI local node=%q started; business readiness available at /readyz: http://localhost%s/webui/ route=%q account=%q model=deepseek",
		configValue.InstanceID, configValue.ListenAddress, configValue.RouteKey, webUILocalAccountID)

	var terminalErr error
	select {
	case <-signalCtx.Done():
	case terminalErr = <-errorsCh:
	}
	health.beginDrain()
	drainGate.Begin()
	logger.Printf("runtime drain started instance=%q timeout=%s", configValue.InstanceID, configValue.DrainTimeout)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configValue.DrainTimeout)
	defer cancelShutdown()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Shutdown(shutdownCtx) }()
	drainDone := make(chan struct{})
	go func() { draining.Wait(); close(drainDone) }()
	select {
	case <-drainDone:
		logger.Printf("runtime drain completed instance=%q", configValue.InstanceID)
	case <-shutdownCtx.Done():
		terminalErr = errors.Join(terminalErr, errors.New("runtime drain timed out; durable work left for takeover"))
	}
	stop()
	if httpErr := <-httpDone; httpErr != nil {
		_ = server.Close()
		terminalErr = errors.Join(terminalErr, httpErr)
	}
	health.stop()
	done := make(chan struct{})
	go func() { background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("webui local shutdown timed out")
		}
	}
	return terminalErr
}
```

## 结论与证据回查

字段及约束见 [design.md](design.md)，故障判定见 [testing-and-acceptance.md](testing-and-acceptance.md)。代码只能证明条件更新和恢复分支存在，真实渠道可见次数仍需客户端证据，不能从 Ledger 一行推断下游接收次数。

## 代码证据与外部演练的对应

| 已摘录的代码 | 能直接审查的性质 | 外部仍需验证 |
|---|---|---|
| loadWeComHAEntryConfig | origin、参数范围和默认值校验 | 部署实际注入值和容器DNS可达。 |
| probe/recordProbe | 并发、epoch与恢复窗口 | 实际延迟下摘除/重加入时间。 |
| serveHTTP/forward | 签名材料保持、拒绝不切换、转发有界 | 真实callback与Inbox重复接收。 |
| gate/runtime drain | 停准入、停止读取、工作有界结束 | 目标工具/渠道是否响应取消。 |
| runtime_files | 读模式不写、原子发布不覆盖 | 主机卷权限、空间与镜像内容一致。 |
| 组合根接线 | 角色接入同一drain信号，结束不重启 | 长期负载、资源趋势与反向故障。 |

## 为什么此处保留完整入口源码

入口的安全性质分散在配置、client构造、状态记录、路由选择和响应读取中。只贴健康布尔值或forward函数会遗漏重定向、逐跳头、旧探测和响应已写后的切换问题。

排空同样不能只看Consumer：组合根若立刻取消活动context，Consumer自己的“读取消/工作取消分离”仍会失去意义。附录保留组合根接线，便于把局部协议与进程生命周期一起审查。
