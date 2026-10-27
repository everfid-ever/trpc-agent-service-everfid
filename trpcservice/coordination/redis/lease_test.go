package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

func TestInspectLeaseIsReadOnlyAtomicAndPreservesLargeFence(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	manager, err := New(client, "inspect-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := coordination.SessionKey{TenantID: "tenant-a", AgentAppID: "app", SessionID: "session"}
	const minimum uint64 = 9007199254740993
	if err := manager.EnsureFenceAtLeast(ctx, key, minimum); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(ctx, key, "node-a-process-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := manager.Inspect(ctx, key)
	if err != nil || !observation.Held || observation.WorkerID != lease.WorkerID || observation.Fence != minimum+1 || observation.RemainingMillis != time.Minute.Milliseconds() {
		t.Fatalf("%+v %v", observation, err)
	}
	other := key
	other.TenantID = "tenant-b"
	if observation, err = manager.Inspect(ctx, other); err != nil || observation.Held || observation.Fence != 0 {
		t.Fatalf("%+v %v", observation, err)
	}
	server.FastForward(2 * time.Minute)
	if observation, err = manager.Inspect(ctx, key); err != nil || observation.Held || observation.Fence != minimum+1 {
		t.Fatalf("%+v %v", observation, err)
	}
}

func TestRedisLeaseFenceContract(t *testing.T) {
	client := redisTestClient(t)
	manager, err := New(client, fmt.Sprintf("lease_test_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	key := coordination.SessionKey{TenantID: "tenant-a", AgentAppID: "app", SessionID: "session"}
	leaseKey, fenceKey := manager.keys(key)
	t.Cleanup(func() { _ = client.Del(context.Background(), leaseKey, fenceKey).Err() })

	first, err := manager.Acquire(context.Background(), key, "worker-1", time.Second)
	if err != nil || first.Fence != 1 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	observation, err := manager.Inspect(context.Background(), key)
	if err != nil || !observation.Held || observation.WorkerID != first.WorkerID || observation.Fence != first.Fence || observation.RemainingMillis <= 0 {
		t.Fatalf("observation=%#v err=%v", observation, err)
	}
	if _, err := manager.Acquire(context.Background(), key, "worker-2", time.Second); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("busy acquire=%v", err)
	}
	forged := first
	forged.WorkerID = "worker-2"
	if _, err := manager.Renew(context.Background(), forged, time.Second); !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatalf("forged renew=%v", err)
	}
	renewed, err := manager.Renew(context.Background(), first, time.Second)
	if err != nil || renewed.ExpiresAt.Before(first.ExpiresAt) {
		t.Fatalf("renewed=%#v err=%v", renewed, err)
	}
	if err := manager.Release(context.Background(), renewed); err != nil {
		t.Fatal(err)
	}
	observation, err = manager.Inspect(context.Background(), key)
	if err != nil || observation.Held || observation.WorkerID != "" || observation.Fence != 1 {
		t.Fatalf("released observation=%#v err=%v", observation, err)
	}
	// Simulate loss/restoration of the Redis coordination dataset. PostgreSQL's
	// durable last_fence is supplied as the calibration minimum.
	if err := client.Del(context.Background(), fenceKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureFenceAtLeast(context.Background(), key, 10); err != nil {
		t.Fatal(err)
	}
	second, err := manager.Acquire(context.Background(), key, "worker-2", time.Second)
	if err != nil || second.Fence != 11 {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	if err := manager.Release(context.Background(), second); err != nil {
		t.Fatal(err)
	}
}

func TestFenceCalibrationIsExactAboveLuaIntegerPrecision(t *testing.T) {
	client := redisTestClient(t)
	manager, err := New(client, fmt.Sprintf("lease_precision_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	key := coordination.SessionKey{TenantID: "tenant-a", AgentAppID: "app", SessionID: "large-fence"}
	leaseKey, fenceKey := manager.keys(key)
	t.Cleanup(func() { _ = client.Del(context.Background(), leaseKey, fenceKey).Err() })
	const durableFence = uint64(9007199254740993)
	if err := manager.EnsureFenceAtLeast(context.Background(), key, durableFence); err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(context.Background(), key, "worker", time.Second)
	if err != nil || lease.Fence != durableFence+1 {
		t.Fatalf("lease=%#v err=%v", lease, err)
	}
}

func TestFenceCalibrationReservesIncrementCapacity(t *testing.T) {
	client := redisTestClient(t)
	manager, err := New(client, fmt.Sprintf("lease_limit_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	key := coordination.SessionKey{TenantID: "tenant-a", AgentAppID: "app", SessionID: "fence-limit"}
	if err := manager.EnsureFenceAtLeast(context.Background(), key, uint64(math.MaxInt64)); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("max fence calibration err=%v", err)
	}
}

func redisTestClient(t *testing.T) *redisclient.Client {
	t.Helper()
	address := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("TRPC_REDIS_TEST_ADDR is not set")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}
