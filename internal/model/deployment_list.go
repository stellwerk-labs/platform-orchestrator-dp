package model

import (
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const deploymentPageTokenMaxLength = 80
const deploymentSearchMaxRunes = 200

func parseDeploymentPageToken(token string) (time.Time, uuid.NullUUID, error) {
	var id uuid.NullUUID
	if len(token) > deploymentPageTokenMaxLength {
		return time.Time{}, id, NewErrBadRequest("invalid deployment page token")
	}
	timestamp, identifier, hasIdentifier := strings.Cut(token, ",")
	stamp, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return time.Time{}, id, NewErrBadRequest("invalid deployment page token")
	}
	if hasIdentifier {
		parsed, err := uuid.Parse(identifier)
		if err != nil {
			return time.Time{}, id, NewErrBadRequest("invalid deployment page token")
		}
		id = uuid.NullUUID{UUID: parsed, Valid: true}
	}
	return time.Unix(0, stamp).UTC(), id, nil
}

func deploymentListSearch(query, filter string) (string, error) {
	if !utf8.ValidString(query) || utf8.RuneCountInString(query) > deploymentSearchMaxRunes || strings.IndexFunc(query, unicode.IsControl) >= 0 {
		return "", NewErrBadRequest("deployment search must contain at most 200 printable characters")
	}
	switch filter {
	case "", "all", "plans", "unused_plans", "deployments", "needs_approval", "needs_attention":
	default:
		return "", NewErrBadRequest("invalid deployment quick filter")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return "", nil
	}
	return "%" + strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(query) + "%", nil
}
