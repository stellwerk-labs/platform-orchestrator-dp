package model

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

// NewErrBadRequest constructs a new bad request error. Use this when the arguments are invalid or malformed.
func NewErrBadRequest(message string) ErrBadRequest {
	return ErrBadRequest{message}
}

// NewErrConflict constructs a conflict error. Use this when the database state is not valid for this operation.
func NewErrConflict(message string) ErrConflict {
	return ErrConflict{Message: message}
}

// Stable conflict codes. Clients branch on them, so they never change meaning.
const (
	CodeEnvironmentBusy       = "environment_busy"
	CodeOutcomeUnreconciled   = "outcome_unreconciled"
	CodeEnvironmentDeleting   = "environment_deleting"
	CodeDeploymentQueueFull   = "deployment_queue_full"
	CodeDeploymentBaseChanged = "deployment_base_changed"
)

// Sentinels for errors.Is. A coded conflict matches its sentinel whatever its message.
var (
	ErrEnvironmentBusy     = ErrConflict{Code: CodeEnvironmentBusy}
	ErrOutcomeUnreconciled = ErrConflict{Code: CodeOutcomeUnreconciled}
)

// NewErrCodedConflict constructs a conflict that carries a stable code.
func NewErrCodedConflict(code, message string) ErrConflict {
	return ErrConflict{Message: message, Code: code}
}

// NewErrNotFound constructs a not found error. Use this when the subject of the request is not found
func NewErrNotFound(message string) ErrNotFound {
	return ErrNotFound{message}
}

type ErrBadRequest struct {
	Message string
}

func (e ErrBadRequest) Error() string {
	return fmt.Sprintf("bad request: %s", e.Message)
}

func IsErrBadRequest(err error) (ErrBadRequest, bool) {
	var e ErrBadRequest
	if errors.As(err, &e) {
		return e, true
	}
	return e, false
}

type ErrConflict struct {
	Message string
	Code    string
	// DeploymentID is the Deployment the conflict is about, if it is about one.
	DeploymentID opt.Opt[uuid.UUID]
}

// AboutDeployment names the Deployment the conflict is about; uuid.Nil names none.
func (e ErrConflict) AboutDeployment(id uuid.UUID) ErrConflict {
	if id != uuid.Nil {
		e.DeploymentID = opt.Of(id)
	}
	return e
}

func (e ErrConflict) Error() string {
	return fmt.Sprintf("conflict: %s", e.Message)
}

func (e ErrConflict) Is(target error) bool {
	sentinel, ok := target.(ErrConflict)
	return ok && sentinel.Code != "" && sentinel.Code == e.Code
}

func IsErrConflict(err error) (ErrConflict, bool) {
	var e ErrConflict
	if errors.As(err, &e) {
		return e, true
	}
	return e, false
}

type ErrNotFound struct {
	Message string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("not found: %s", e.Message)
}

func IsErrNotFound(err error) (ErrNotFound, bool) {
	var e ErrNotFound
	if errors.As(err, &e) {
		return e, true
	}
	return e, false
}
