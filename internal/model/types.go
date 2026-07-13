package model

import "time"

type MetadataKey struct {
	Name        string
	Description *string
	Schema      MetadataKeySchema
	CreatedAt   time.Time
}

type MetadataKeySchema struct {
	Type    string
	Format  *string
	Pattern *string
}
