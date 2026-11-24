//go:build integration

package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

// Drop only a successful PostgreSQL COMMIT response for the replan lineage transaction.
// All SQL, constraints, admission preparation and durable rows use real PostgreSQL.
func producerCommitLossProxy(t *testing.T, f *queueFixture) (string, <-chan struct{}) {
	t.Helper()
	config, err := pq.NewConfig(f.connection)
	require.NoError(t, err)
	require.Empty(t, config.Multi, "commit-loss proxy requires exactly one PostgreSQL endpoint")
	address := config.Host
	if config.Hostaddr.IsValid() {
		address = config.Hostaddr.String()
	}
	ip := net.ParseIP(address)
	require.NotNil(t, ip, "commit-loss proxy requires a literal PostgreSQL address")
	require.True(t, ip.IsLoopback(), "commit-loss proxy requires an owned loopback PostgreSQL fixture")
	require.NotZero(t, config.Port)
	port := strconv.Itoa(int(config.Port))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lost := make(chan struct{}, 1)
	var once atomic.Bool
	var workers sync.WaitGroup
	var mu sync.Mutex
	var connections []net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			downstream, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.DialTimeout("tcp", net.JoinHostPort(address, port), time.Second)
			if err != nil {
				_ = downstream.Close()
				continue
			}
			mu.Lock()
			connections = append(connections, downstream, upstream)
			mu.Unlock()
			_ = downstream.SetDeadline(time.Now().Add(45 * time.Second))
			_ = upstream.SetDeadline(time.Now().Add(45 * time.Second))
			var dropping atomic.Bool
			workers.Add(2)
			go func() {
				defer workers.Done()
				defer func() { _ = downstream.Close() }()
				defer func() { _ = upstream.Close() }()
				var startup [4]byte
				if _, err := io.ReadFull(downstream, startup[:]); err != nil {
					return
				}
				size := binary.BigEndian.Uint32(startup[:])
				if size < 4 || size > 1<<20 {
					return
				}
				body := make([]byte, int(size)-4)
				if _, err := io.ReadFull(downstream, body); err != nil {
					return
				}
				if _, err := upstream.Write(append(startup[:], body...)); err != nil {
					return
				}
				lineage := false
				for {
					frame, err := producerPGFrame(downstream)
					if err != nil {
						return
					}
					if bytes.Contains(frame[5:], []byte("UPDATE deployment_pipeline_runs SET replanned_from_id")) {
						lineage = true
					}
					if lineage && frame[0] == 'Q' && string(frame[5:]) == "COMMIT\x00" && once.CompareAndSwap(false, true) {
						dropping.Store(true)
					}
					if _, err = upstream.Write(frame); err != nil {
						return
					}
				}
			}()
			go func() {
				defer workers.Done()
				defer func() { _ = downstream.Close() }()
				defer func() { _ = upstream.Close() }()
				for {
					frame, err := producerPGFrame(upstream)
					if err != nil {
						return
					}
					if dropping.Load() && frame[0] == 'C' && string(frame[5:]) == "COMMIT\x00" {
						lost <- struct{}{}
						return
					}
					if _, err = downstream.Write(frame); err != nil {
						return
					}
				}
			}()
		}
	}()
	connection := f.connection
	_, proxyPort, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	if strings.HasPrefix(connection, "postgres://") || strings.HasPrefix(connection, "postgresql://") {
		parsed, err := url.Parse(connection)
		require.NoError(t, err)
		parsed.Host = net.JoinHostPort("127.0.0.1", proxyPort)
		query := parsed.Query()
		query.Set("host", "127.0.0.1")
		query.Set("hostaddr", "127.0.0.1")
		query.Set("port", proxyPort)
		query.Set("sslmode", "disable")
		query.Set("search_path", f.schema)
		parsed.RawQuery = query.Encode()
		return parsed.String(), lost
	}
	return connection + " host=127.0.0.1 hostaddr=127.0.0.1 port=" + proxyPort + " sslmode=disable search_path=" + f.schema, lost
}

func TestProducerCommitLossProxyPreservesConnectionParameters(t *testing.T) {
	for _, trial := range []struct {
		name       string
		connection string
	}{
		{"keyword", "host=127.0.0.2 hostaddr=127.0.0.3 port=1 user=proxy_trial password='a=b@c:d' dbname=proxy_baseline sslmode=require search_path=old_scope application_name='proxy parser' options='-c statement_timeout=3210'"},
		{"URL with endpoint overrides", "postgres://proxy_trial:a%3Db%40c%3Ad@127.0.0.2:1/proxy_baseline?host=127.0.0.4&hostaddr=127.0.0.3&port=2&sslmode=require&search_path=old_scope&application_name=proxy+parser&options=-c+statement_timeout%3D3210"},
	} {
		t.Run(trial.name, func(t *testing.T) {
			before, err := pq.NewConfig(trial.connection)
			require.NoError(t, err)
			connection, _ := producerCommitLossProxy(t, &queueFixture{connection: trial.connection, schema: "proxy_scope"})
			after, err := pq.NewConfig(connection)
			require.NoError(t, err)
			require.Equal(t, before.User, after.User)
			require.Equal(t, before.Password, after.Password)
			require.Equal(t, before.Database, after.Database)
			require.Equal(t, before.ApplicationName, after.ApplicationName)
			require.Equal(t, before.Options, after.Options)
			require.Equal(t, "127.0.0.1", after.Host)
			require.Equal(t, "127.0.0.1", after.Hostaddr.String())
			require.NotZero(t, after.Port)
			require.NotEqual(t, before.Port, after.Port)
			require.Equal(t, pq.SSLModeDisable, after.SSLMode)
			require.Equal(t, "proxy_scope", after.Runtime["search_path"])
			require.Empty(t, after.Multi)
		})
	}
}

func producerPGFrame(r io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size < 4 || size > 16<<20 {
		return nil, fmt.Errorf("invalid PostgreSQL fixture frame length %d", size)
	}
	frame := make([]byte, int(size)+1)
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[5:])
	return frame, err
}

func TestNativeProducerReplanRecoversCommittedTransactionAfterResponseLoss(t *testing.T) {
	f := newNativeProducerFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
	connection, lost := producerCommitLossProxy(t, f)
	store, err := model.NewDatabaser(f.ctx, f.server.Logger, connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	f.server.Database = store
	_, err = f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "commit-lost")
	require.Error(t, err)
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("no successful server COMMIT was observed")
	}
	f.server.Database = f.store
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE replanned_from_id=$1`, plan.Id))
	control := f.server.ControlPlaneClient
	f.server.ControlPlaneClient = nil
	recovered, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "commit-lost")
	f.server.ControlPlaneClient = control
	require.NoError(t, err)
	receipt := nativeProducerReceipt(t, f, recovered.Id)
	require.Equal(t, "committed", receipt.Disposition)
	require.False(t, receipt.CoreSettledAt.Valid, "commit uncertainty must not fabricate absence or completion")
	require.NotEqual(t, uuid.Nil, recovered.Id)
	require.Equal(t, 2, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_admission_receipts WHERE disposition='absent'`))
}
