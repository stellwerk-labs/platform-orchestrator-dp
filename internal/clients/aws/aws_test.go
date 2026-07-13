package aws

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

func TestIsAssumeRoleError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "InvalidIdentityToken error",
			err:      &smithy.OperationError{Err: errors.New("InvalidIdentityToken: No OpenIDConnect provider found in your account for https://oidc.platform-orchestrator.dev")},
			expected: true,
		},
		{
			name:     "AccessDenied error",
			err:      &smithy.OperationError{Err: errors.New("AccessDenied: User is not authorized to perform: sts:AssumeRoleWithWebIdentity")},
			expected: true,
		},
		{
			name:     "AccessDeniedException error",
			err:      &smithy.OperationError{Err: errors.New("AccessDeniedException: Access denied")},
			expected: true,
		},
		{
			name:     "ExpiredToken error",
			err:      &smithy.OperationError{Err: errors.New("ExpiredToken: The security token included in the request is expired")},
			expected: true,
		},
		{
			name:     "ExpiredTokenException error",
			err:      &smithy.OperationError{Err: errors.New("ExpiredTokenException: Token expired")},
			expected: true,
		},
		{
			name:     "IDPRejectedClaim error",
			err:      &smithy.OperationError{Err: errors.New("IDPRejectedClaim: The identity provider rejected the claim")},
			expected: true,
		},
		{
			name:     "InvalidParameterValue error",
			err:      &smithy.OperationError{Err: errors.New("InvalidParameterValue: Invalid role ARN")},
			expected: true,
		},
		{
			name:     "MalformedPolicyDocument error",
			err:      &smithy.OperationError{Err: errors.New("MalformedPolicyDocument: Policy document is malformed")},
			expected: true,
		},
		{
			name:     "PackedPolicyTooLarge error",
			err:      &smithy.OperationError{Err: errors.New("PackedPolicyTooLarge: Policy is too large")},
			expected: true,
		},
		{
			name:     "RegionDisabledException error",
			err:      &smithy.OperationError{Err: errors.New("RegionDisabledException: Region is disabled")},
			expected: true,
		},
		{
			name:     "HTTP 400 status code",
			err:      errors.New("operation error STS: AssumeRoleWithWebIdentity, https response error StatusCode: 400, RequestID: abc123"),
			expected: true,
		},
		{
			name:     "HTTP 403 status code",
			err:      errors.New("operation error STS: AssumeRoleWithWebIdentity, https response error StatusCode: 403, RequestID: abc123"),
			expected: true,
		},
		{
			name:     "HTTP 404 status code",
			err:      errors.New("operation error STS: AssumeRoleWithWebIdentity, https response error StatusCode: 404, RequestID: abc123"),
			expected: true,
		},
		{
			name:     "HTTP 500 status code - not a user error",
			err:      errors.New("operation error STS: AssumeRoleWithWebIdentity, https response error StatusCode: 500, RequestID: abc123"),
			expected: false,
		},
		{
			name:     "generic error - not STS related",
			err:      errors.New("some other error"),
			expected: false,
		},
		{
			name:     "smithy.OperationError without STS error code",
			err:      &smithy.OperationError{Err: errors.New("some other AWS error")},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAssumeRoleError(tt.err)
			assert.Equal(t, tt.expected, result, "IsAssumeRoleError(%v) = %v, expected %v", tt.err, result, tt.expected)
		})
	}
}

func TestIsAssumeRoleError_WrappedErrors(t *testing.T) {
	// Test that wrapped errors are properly detected
	baseErr := &smithy.OperationError{Err: errors.New("InvalidIdentityToken: No OpenIDConnect provider found")}
	wrappedErr := fmt.Errorf("failed to assume role: %w", baseErr)

	result := IsAssumeRoleError(wrappedErr)
	assert.True(t, result, "IsAssumeRoleError should detect wrapped smithy.OperationError")
}

func TestIsAccessDenied(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "AccessDenied error",
			err:      &smithy.OperationError{Err: errors.New("AccessDenied: User is not authorized")},
			expected: true,
		},
		{
			name:     "other error",
			err:      &smithy.OperationError{Err: errors.New("SomeOtherError")},
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, result := IsAccessDenied(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAccessDeniedException(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "AccessDeniedException error",
			err:      &smithy.OperationError{Err: errors.New("AccessDeniedException: Access denied")},
			expected: true,
		},
		{
			name:     "other error",
			err:      &smithy.OperationError{Err: errors.New("SomeOtherError")},
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, result := IsAccessDeniedException(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
