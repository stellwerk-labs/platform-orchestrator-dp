package model

import (
	"context"
	"database/sql"
	"embed"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/golib/hpostgresconnect"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
)

//go:generate go tool mockgen  -destination mocks/databaser.go github.com/stellwerk-labs/platform-orchestrator-dp/internal/model Databaser,TxWithCommit

//go:embed migrations/*.sql
var embedMigrations embed.FS

// Model is the underlying type for the entire model.
type databaser struct {
	*sql.DB
	logger *zap.Logger
}

type gooseZapLogger struct {
	*zap.SugaredLogger
}

func (g *gooseZapLogger) Printf(format string, v ...interface{}) {
	g.Infof(format, v...)
}

// NewDatabaser creates a new database provider instance
func NewDatabaser(ctx context.Context, logger *zap.Logger, connStr string) (Databaser, error) {
	goose.SetLogger(&gooseZapLogger{SugaredLogger: logger.Named("goose").Sugar()})
	goose.SetBaseFS(embedMigrations)
	goose.SetVerbose(logger.Level() <= zap.DebugLevel)
	goose.AddNamedMigrationContext("000002_pending_event_messages.go", hstandardoutbox.MigrateUp01, hstandardoutbox.MigrateDown01)

	if inner, err := hpostgresconnect.InitDatabase(ctx, &hpostgresconnect.Config{
		Logger:  logger,
		ConnStr: connStr,
	}); err != nil {
		return nil, err
	} else if err := goose.Up(inner.DB, "migrations"); err != nil {
		return nil, err
	} else {
		return &databaser{DB: inner.DB, logger: logger}, nil
	}
}

// Databaser provides an interface which can be used to mock the model
type Databaser interface {
	CreateMetadataKey(ctx context.Context, optionalTx Tx, orgId string, key *MetadataKey) (*MetadataKey, error)
	GetMetadataKey(ctx context.Context, optionalTx Tx, orgId string, keyName string) (*MetadataKey, error)
	ListMetadataKeys(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int) ([]*MetadataKey, string, error)
	UpdateMetadataKey(ctx context.Context, optionalTx Tx, orgId string, key *MetadataKey) error
	DeleteMetadataKey(ctx context.Context, optionalTx Tx, orgId string, keyName string) error

	AsReliableOutboxStore() reliableoutbox.Store[*hstandardoutbox.PendingEventMessage]
	InsertPendingEventMessages(ctx context.Context, optionalTx Tx, messages []*hstandardoutbox.PendingEventMessage) ([]*hstandardoutbox.PendingEventMessage, error)
	Close() error

	BeginTx(ctx context.Context, opts *sql.TxOptions) (TxWithCommit, error)

	CreateDeployment(ctx context.Context, optionalTx Tx, orgId, projectId, envId string, params CreateDeploymentParams) (*DeploymentSummary, error)
	ListDeployments(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListDeploymentsParams) ([]DeploymentSummary, string, error)
	ListLastDeployments(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListLastDeploymentsParams) ([]DeploymentSummary, string, error)
	GetDeployment(ctx context.Context, optionalTx Tx, orgId string, deploymentId uuid.UUID, mode GetMode) (*DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error)
	GetDeploymentEncryptedOutputs(ctx context.Context, optionalTx Tx, orgId string, deploymentId uuid.UUID) (string, error)
	GetDeploymentByIdempotencyKeyDigest(ctx context.Context, optionalTx Tx, orgId, projectId, envId, idempotencyKeyDigest string) (*DeploymentSummary, EncodedDeploymentManifest, error)
	GetLastDeployment(ctx context.Context, optionalTx Tx, orgId string, projectId, envId string, params GetLastDeploymentParams) (*DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error)
	DeleteDeploymentsForEnv(ctx context.Context, optionalTx Tx, orgId, projectId, envId string, force bool) error
	UpdateDeploymentStatusAndOutputs(ctx context.Context, optionalTx Tx, deploymentId uuid.UUID, params UpdateDeploymentStatusAndOutputsParams) (*DeploymentSummary, error)
	DeleteDeploymentOutputsByCompletionDate(ctx context.Context, optionalTx Tx, completedBefore time.Time) ([]DeploymentSummary, error)
	ListLastDeploymentsByNodeProperties(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListLastDeploymentsByNodePropertiesParams) ([]DeploymentSummary, string, error)
	CreateDeploymentHistoryRecord(ctx context.Context, optionalTx Tx, dep *DeploymentSummary) error
	TryRecordRunnerEvent(ctx context.Context, optionalTx Tx, eventID, orgID, runnerID string, deploymentID uuid.UUID, eventType string) (bool, error)

	InitActiveResourcesFromGraph(ctx context.Context, tx Tx, deploymentEnvUuid, deploymentId uuid.UUID, graph *platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]) error
	GetActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid uuid.UUID) ([]ResourceNode, error)
	DiscardOldActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid uuid.UUID, deploymentId uuid.UUID) error
	BulkUpdateActiveResources(ctx context.Context, optionalTx Tx, deploymentEnvUuid, deploymentId uuid.UUID, params []UpdateResourceNodeParams) error
}

func (d *databaser) AsReliableOutboxStore() reliableoutbox.Store[*hstandardoutbox.PendingEventMessage] {
	return hstandardoutbox.SQLContextAsReliableOutbox(d.DB)
}

func (d *databaser) InsertPendingEventMessages(ctx context.Context, optionalTx Tx, messages []*hstandardoutbox.PendingEventMessage) ([]*hstandardoutbox.PendingEventMessage, error) {
	return hstandardoutbox.InsertPendingEventMessages(ctx, d.txOrDb(optionalTx), messages)
}

type Tx interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type TxWithCommit interface {
	Tx
	Commit() error
	Rollback() error
}

func (d *databaser) txOrDb(optionalTx Tx) Tx {
	if optionalTx == nil {
		return d
	}
	return optionalTx
}

func (d *databaser) BeginTx(ctx context.Context, opts *sql.TxOptions) (TxWithCommit, error) {
	return d.DB.BeginTx(ctx, opts)
}

type GetMode string

const (
	GetModeDefault   GetMode = "default"
	GetModeForUpdate GetMode = "for_update"
)

func GetModeSuffix(mode GetMode) string {
	switch mode {
	case GetModeForUpdate:
		return " FOR UPDATE"
	default:
		return ""
	}
}
