package opt

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionalString(t *testing.T) {
	var o Opt[string]
	assert.False(t, o.IsSet())
	assert.Panics(t, func() {
		_ = o.Must()
	})
	assert.Nil(t, o.Ref())
	assert.False(t, o.Map(func(s string) string { return s }).IsSet())
	assert.Equal(t, "x", o.OrFunc(func() string {
		return "x"
	}))
}

func TestOfNonZero(t *testing.T) {
	assert.Equal(t, OfNonZero(0), Empty[int]())
	assert.Equal(t, OfNonZero(1), Of(1))
	assert.Equal(t, OfNonZero(""), Empty[string]())
	assert.Equal(t, OfNonZero("hi"), Of("hi"))
}

func TestOptionalStringOf(t *testing.T) {
	o := Of("x")
	assert.True(t, o.IsSet())
	assert.Equal(t, "x", o.Must())
	assert.NotNil(t, o.Ref())
	assert.Equal(t, Of("x"), o)
	assert.Equal(t, "X", o.Map(func(s string) string { return strings.ToUpper(s) }).Must())
	assert.Equal(t, "x", o.OrFunc(func() string {
		return "y"
	}))
}

func TestOptionalStringOfRef(t *testing.T) {
	o := OfRef[string](nil)
	assert.False(t, o.IsSet())
	x := "x"
	o = OfRef(&x)
	assert.True(t, o.IsSet())
	x = "y"
	assert.Equal(t, "x", o.Must())
}

func TestCompare(t *testing.T) {
	assert.Equal(t, 0, Compare[string](Of("x"), Of("x")))
	assert.Equal(t, 0, Compare[string](Empty[string](), Empty[string]()))

	assert.Equal(t, -1, Compare[string](Of("x"), Of("y")))
	assert.Equal(t, -1, Compare[string](Empty[string](), Of("y")))

	assert.Equal(t, 1, Compare[string](Of("y"), Of("x")))
	assert.Equal(t, 1, Compare[string](Of("y"), Empty[string]()))
}

func TestJson_set(t *testing.T) {
	x := Of("x")
	raw, err := json.Marshal(x)
	require.NoError(t, err)
	var out Opt[string]
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, x, out)
}

func TestJson_not_set(t *testing.T) {
	x := OfRef[string](nil)
	raw, err := json.Marshal(x)
	require.NoError(t, err)
	var out Opt[string]
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, x, out)
}
