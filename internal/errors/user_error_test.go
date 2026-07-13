package errors

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestUserError(t *testing.T) {
	t.Run("Error() returns the error message", func(t *testing.T) {
		err := NewUserError("test error")
		assert.Equal(t, "test error", err.Error())
	})

	t.Run("Error() returns the error message with details", func(t *testing.T) {
		rootErr := errors.New("root cause")
		err := NewUserErrorWithDetails("test error", rootErr)

		assert.Equal(t, "test error: root cause", err.Error())
	})

	t.Run("Unwrap() returns the wrapped error", func(t *testing.T) {
		rootErr := errors.New("root cause")
		err := NewUserErrorWithDetails("test error", rootErr)

		assert.Equal(t, "test error", errors.Unwrap(err).Error())
	})
}
