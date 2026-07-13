package opt

import (
	"cmp"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Opt is a generic optional structure used in our control plane code. This should be used whenever you are tempted
// to use a primitive pointer or a primitive where the zero value indicates "not set".
// This type provides a bunch of useful functions for working with optional values including for converting them
// to the sql scan and value types.
type Opt[x comparable] struct {
	value x
	isSet bool
}

// Of returns a valid non-null copy of the value
func Of[x comparable](value x) Opt[x] {
	return Opt[x]{value: value, isSet: true}
}

// Empty returns a not-set value
func Empty[x comparable]() Opt[x] {
	return Opt[x]{isSet: false}
}

// OfRef converts the pointer primitive to the optional type
func OfRef[x comparable](value *x) Opt[x] {
	if value == nil {
		return Opt[x]{isSet: false}
	}
	return Opt[x]{value: *value, isSet: true}
}

// OfNonZero returns a set value if the passed in value was not the zero value for that type
func OfNonZero[x comparable](value x) Opt[x] {
	var zero x
	if value == zero {
		return Empty[x]()
	}
	return Of(value)
}

// Must returns the inner value if it is set
func (o Opt[x]) Must() x {
	if o.isSet {
		return o.value
	}
	panic("optional value is not set")
}

// IsSet returns true if the inner value is set, otherwise false
func (o Opt[x]) IsSet() bool {
	return o.isSet
}

// Or returns the inner value if set, otherwise the given fallback value
func (o Opt[x]) Or(defaultValue x) x {
	if o.isSet {
		return o.value
	}
	return defaultValue
}

// OrOpt returns the same if set, or returns the possible value as an alternative
func (o Opt[x]) OrOpt(possible Opt[x]) Opt[x] {
	if o.isSet {
		return o
	}
	return possible
}

// OrFunc returns the inner value if set, otherwise calls the given function to get a default value
func (o Opt[x]) OrFunc(defaultValue func() x) x {
	if o.isSet {
		return o.value
	}
	return defaultValue()
}

// Ref returns a pointer to the value if set
func (o Opt[x]) Ref() *x {
	if o.isSet {
		return &o.value
	}
	return nil
}

func (o Opt[x]) Map(f func(x) x) Opt[x] {
	if o.isSet {
		return Of(f(o.value))
	}
	return o
}

// Value returns the sql value for the instance, if you are trying to access the inner value, use IsSet and Must instead.
func (o Opt[x]) Value() (driver.Value, error) {
	if o.isSet {
		return o.value, nil
	}
	return nil, nil
}

func (o Opt[x]) Scan(src any) error {
	return fmt.Errorf("cannot scan into a Opt[x] directly, use opt.Scan")
}

func (o Opt[x]) MarshalJSON() ([]byte, error) {
	if o.isSet {
		return json.Marshal(o.value)
	}
	return []byte("null"), nil
}

func (o *Opt[x]) UnmarshalJSON(data []byte) error {
	var zero x
	if string(data) == "null" {
		o.isSet = false
		o.value = zero
	} else if err := json.Unmarshal(data, &zero); err != nil {
		return err
	} else {
		o.isSet = true
		o.value = zero
	}
	return nil
}

// Compare compares optional values, with set values coming before unset values.
func Compare[x cmp.Ordered](a, b Opt[x]) int {
	if a.isSet {
		if b.isSet {
			return cmp.Compare(a.value, b.value)
		}
		return 1
	} else if b.IsSet() {
		return -1
	}
	return 0
}

// Scan converts an optional to a sql scannable type, this is best used to wrap types in the Row.Scan parameters.
func Scan[x comparable](y *Opt[x]) sql.Scanner {
	if y == nil {
		panic("cannot be nil")
	}
	return &scan[x]{ptr: y}
}

type scan[x comparable] struct {
	ptr *Opt[x]
}

func (s *scan[x]) Scan(src any) error {
	var inner sql.Null[x]
	if err := inner.Scan(src); err != nil {
		return err
	}
	s.ptr.value = inner.V
	s.ptr.isSet = inner.Valid
	return nil
}
