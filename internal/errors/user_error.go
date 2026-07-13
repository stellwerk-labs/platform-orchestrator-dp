package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

type UserError struct {
	error
	Details error
}

func (e *UserError) Error() string {
	if e.Details != nil {
		return fmt.Sprintf("%s: %s", e.error, e.Details)
	}
	return e.error.Error()
}

func (e *UserError) Unwrap() error {
	return e.error
}

func NewUserError(message string) error {
	return NewUserErrorWithDetails(message, nil)
}

func NewUserErrorWithDetails(message string, details error) error {
	return &UserError{error: errors.New(message), Details: details}
}
