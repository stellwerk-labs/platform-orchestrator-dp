package model

import (
	"fmt"

	"github.com/pkg/errors"
)

// NewErrBadRequest constructs a new bad request error. Use this when the arguments are invalid or malformed.
func NewErrBadRequest(message string) ErrBadRequest {
	return ErrBadRequest{message}
}

// NewErrConflict constructs a conflict error. Use this when the database state is not valid for this operation.
func NewErrConflict(message string) ErrConflict {
	return ErrConflict{message}
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
}

func (e ErrConflict) Error() string {
	return fmt.Sprintf("conflict: %s", e.Message)
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
