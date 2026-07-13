package model

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

func asJson[x any](v *x) *asJsonInner[x] {
	return &asJsonInner[x]{ptr: v}
}

type asJsonInner[x any] struct {
	ptr *x
}

func (a asJsonInner[x]) Scan(src any) error {
	var zero x
	if src == nil {
		*a.ptr = zero
		return nil
	}
	if raw, ok := src.([]byte); ok {
		if err := json.Unmarshal(raw, &zero); err != nil {
			return err
		}
		*a.ptr = zero
		return nil
	}
	return errors.New("type assertion to []byte failed")
}

func (a asJsonInner[x]) Value() (driver.Value, error) {
	if a.ptr == nil {
		return nil, nil
	}
	raw, err := json.Marshal(a.ptr)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

var _ sql.Scanner = (*asJsonInner[map[string]string])(nil)
var _ driver.Valuer = (*asJsonInner[map[string]string])(nil)
