package taskflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var ErrEvidenceRejected = errors.New("task step tool output does not satisfy its declared evidence check")

// ResultCheck is a deterministic predicate over the verified tool receipt.
// It does not turn a provider receipt into proof of unrelated external facts.
type ResultCheck struct {
	Path       []string `json:"path"`
	EqualsJSON string   `json:"equals_json,omitempty"`
	NonEmpty   bool     `json:"non_empty,omitempty"`
}

// Valid lets reviewed adapters reject malformed evidence contracts at registration.
func (c *ResultCheck) Valid() bool { return c.valid() }

func (c *ResultCheck) valid() bool {
	if c == nil {
		return true
	}
	if len(c.Path) > 8 || (c.EqualsJSON != "") == c.NonEmpty || len(c.EqualsJSON) > 1024 {
		return false
	}
	for _, p := range c.Path {
		if len(p) == 0 || len(p) > 128 {
			return false
		}
	}
	return c.EqualsJSON == "" || json.Valid([]byte(c.EqualsJSON))
}
func decodeJSON(b []byte) (any, error) {
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	err := d.Decode(&v)
	if err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, ErrEvidenceRejected
	}
	return v, nil
}
func (c *ResultCheck) verify(b []byte) error {
	if c == nil {
		return nil
	}
	if !c.valid() {
		return ErrEvidenceRejected
	}
	v, err := decodeJSON(b)
	if err != nil {
		return ErrEvidenceRejected
	}
	for _, p := range c.Path {
		object, ok := v.(map[string]any)
		if !ok {
			return ErrEvidenceRejected
		}
		v, ok = object[p]
		if !ok {
			return ErrEvidenceRejected
		}
	}
	if c.NonEmpty {
		switch x := v.(type) {
		case string:
			if x != "" {
				return nil
			}
		case []any:
			if len(x) > 0 {
				return nil
			}
		case map[string]any:
			if len(x) > 0 {
				return nil
			}
		case bool:
			if x {
				return nil
			}
		}
		return ErrEvidenceRejected
	}
	expected, err := decodeJSON([]byte(c.EqualsJSON))
	if err != nil {
		return ErrEvidenceRejected
	}
	left, _ := json.Marshal(v)
	right, _ := json.Marshal(expected)
	if !bytes.Equal(left, right) {
		return ErrEvidenceRejected
	}
	return nil
}
