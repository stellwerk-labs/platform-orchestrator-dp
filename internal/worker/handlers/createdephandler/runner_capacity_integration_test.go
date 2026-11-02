//go:build integration

package createdephandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	appk8s "github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

type liveCapacityCluster struct{ client appk8s.KubernetesInterface }

func (c liveCapacityCluster) GetClusterClient(context.Context, cp.InternalRunner, vault.VaultClientInterface) (appk8s.KubernetesInterface, error) {
	return c.client, nil
}

func TestRunnerCapacityGatesRealKubernetesAndDurableAgentCommands(t *testing.T) {
	kubeconfig, natsURL := os.Getenv("RUNNER_CAPACITY_TEST_KUBECONFIG"), os.Getenv("RUNNER_CAPACITY_TEST_NATS_URL")
	if kubeconfig == "" || natsURL == "" {
		t.Skip("explicit isolated Kubernetes/NATS runtime fixture required")
	}
	for _, runnerType := range []string{"kubernetes", "kubernetes-agent"} {
		t.Run(runnerType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
			require.NotEmpty(t, connection)
			admin, err := sql.Open("postgres", connection)
			require.NoError(t, err)
			defer func() { require.NoError(t, admin.Close()) }()
			schema := "capacity_dispatch_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
			require.NoError(t, err)
			defer func() {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
				defer cancel()
				_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
				require.NoError(t, err)
			}()
			db, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
			require.NoError(t, err)
			restConfig.Timeout = 10 * time.Second
			cluster, err := clientset.NewForConfig(restConfig)
			require.NoError(t, err)
			namespace := "drift-capacity-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			_, err = cluster.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
			require.NoError(t, err)
			defer func() {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
				defer cancel()
				require.NoError(t, cluster.CoreV1().Namespaces().Delete(cleanup, namespace, metav1.DeleteOptions{}))
				require.Eventually(t, func() bool {
					_, err := cluster.CoreV1().Namespaces().Get(cleanup, namespace, metav1.GetOptions{})
					return apierrors.IsNotFound(err)
				}, 40*time.Second, 200*time.Millisecond)
			}()
			orgID := "capacity-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
			configuration := map[string]any{"type": runnerType, "max_parallel_jobs": 2, "supported_protocols": []string{"environment-observation/v1"}, "job": map[string]any{"namespace": namespace, "service_account": "default"}}
			core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/internal/orgs/"+orgID+"/runners/runner", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": "runner", "org_id": orgID, "runner_configuration": configuration}))
			}))
			defer core.Close()
			cpClient, err := cp.NewClientWithResponses(core.URL)
			require.NoError(t, err)
			nc, err := nats.Connect(natsURL)
			require.NoError(t, err)
			defer nc.Close()
			js, err := jetstream.New(nc)
			require.NoError(t, err)
			require.NoError(t, hnats.EnsureStandardStreams(ctx, js, 1))
			subject, err := hmessaging.RunnerCommandSubject(orgID, "runner")
			require.NoError(t, err)
			stream, err := js.Stream(ctx, hmessaging.RunnerCommandsStreamName)
			require.NoError(t, err)
			consumerName := "capacity_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: consumerName, FilterSubject: subject, AckPolicy: jetstream.AckExplicitPolicy})
			require.NoError(t, err)
			defer func() { require.NoError(t, stream.DeleteConsumer(context.WithoutCancel(ctx), consumerName)) }()
			commandPublisher := &runners.NATSRemoteRunnerCommandPublisher{Publisher: hnats.NewPublisher(js, hmessaging.RunnerCommandsStreamName, zap.NewNop())}
			publisher := &hmessaging.RecordingPublisher{}
			factory := runners.NewDefaultRunnerFactory(zap.NewNop(), nil, liveCapacityCluster{appk8s.NewKubernetesClient(cluster)}, nil, "registry.k8s.io/pause:3.10", "", commandPublisher, time.Minute, time.Hour)
			handler := &CreateDepHandler{db: db, publisher: publisher, controlPlaneClient: cpClient, runnerFactory: factory}
			var deployments []*model.DeploymentSummary
			for range 3 {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				dep, err := db.CreateDeployment(
					ctx,
					tx,
					orgID,
					"p",
					uuid.NewString(),
					model.CreateDeploymentParams{
						CreatedBy:         uuid.New(),
						DeploymentEnvUuid: uuid.New(),
						Mode:              model.DeploymentModeDeploy,
						RunnerId:          "runner",
						RunnerLogLevel:    "info",
						Manifest:          model.EncodedDeploymentManifest(`{}`),
						Graph:             model.EncodedDeploymentGraph(`{}`),
						Tofu:              model.RawTofu("terraform {}"),
					},
				)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				deployments = append(deployments, dep)
			}
			handle := func(dep *model.DeploymentSummary) error {
				return handler.Handle(ctx, zap.NewNop(), hmessaging.Delivery{Message: hmessaging.Message{Data: buildDepEvent(t, orgID, dep.Id)}})
			}
			require.NoError(t, handle(deployments[0]))
			require.NoError(t, handle(deployments[1]))
			blocked := handle(deployments[2])
			require.Error(t, blocked)
			_, retry := hmessaging.AsRetryError(blocked)
			require.True(t, retry, "ordinary work waits in existing dispatch path")
			budget, err := db.GetDeploymentDispatchBudget(ctx, nil, deployments[2].Id)
			require.NoError(t, err)
			require.False(t, budget.FirstAttemptAt.Valid, "full capacity cannot begin a Runner send")
			require.NoError(t, handle(deployments[0]), "redelivery reuses exact owned slot and idempotent job/command")
			if runnerType == "kubernetes" {
				jobs, err := cluster.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Len(t, jobs.Items, 2)
				for _, job := range jobs.Items {
					require.Contains(t, []string{deployments[0].Id.String(), deployments[1].Id.String()}, job.Name)
				}
			} else {
				info, err := consumer.Info(ctx)
				require.NoError(t, err)
				require.Equal(t, uint64(2), info.NumPending, "exact durable command identities deduplicate redelivery")
			}
			// Authoritative local fixture stop: remove the first actual Job before
			// recording its authenticated result. The remote command is inspected below.
			if runnerType == "kubernetes" {
				foreground := metav1.DeletePropagationForeground
				require.NoError(t, cluster.BatchV1().Jobs(namespace).Delete(ctx, deployments[0].Id.String(), metav1.DeleteOptions{PropagationPolicy: &foreground}))
				require.Eventually(t, func() bool {
					_, err := cluster.BatchV1().Jobs(namespace).Get(ctx, deployments[0].Id.String(), metav1.GetOptions{})
					return apierrors.IsNotFound(err)
				}, 20*time.Second, 100*time.Millisecond)
			}
			_, err = db.UpdateDeploymentStatusAndOutputs(ctx, nil, deployments[0].Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
			require.NoError(t, err)
			require.NoError(t, handle(deployments[2]))
			if runnerType == "kubernetes" {
				jobs, err := cluster.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Len(t, jobs.Items, 2)
			} else {
				messages, err := consumer.Fetch(3, jetstream.FetchMaxWait(time.Second))
				require.NoError(t, err)
				count := 0
				for message := range messages.Messages() {
					var envelope hmessaging.CommandEnvelope
					require.NoError(t, json.Unmarshal(message.Data(), &envelope))
					require.Equal(t, orgID, envelope.OrganizationID)
					require.Equal(t, "runner", envelope.RunnerID)
					require.Equal(t, hmessaging.CommandTypeCreateJob, envelope.Type)
					require.Contains(t, envelope.CommandID, "create-job:")
					require.NoError(t, message.Ack())
					count++
				}
				require.NoError(t, messages.Error())
				require.Equal(t, 3, count)
			}
			t.Logf("qualified %s shared limit2 with real PostgreSQL and %s", runnerType, fmt.Sprintf("Kubernetes %s / durable NATS", restConfig.Host))
		})
	}
}
