package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
)

// maxFormContentBytes bounds the answer of one form request.
const maxFormContentBytes = 64 << 10

// formSchema is a flat elicitation schema: an object whose properties are
// strings, numbers, integers, booleans or enums.
type formSchema struct {
	props    map[string]formProperty
	required []string
}

type formProperty struct {
	typ  string
	enum []any // nil = no enum
}

// parseFormSchema reads a requestedSchema. A schema that is not flat gives
// an error: goclaw cannot show it as a form.
func parseFormSchema(raw json.RawMessage) (*formSchema, error) {
	var s struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("requested schema is not a JSON object: %w", err)
	}
	if s.Type != "object" {
		return nil, errors.New("requested schema type is not object")
	}
	fs := &formSchema{props: make(map[string]formProperty, len(s.Properties)), required: s.Required}
	for name, rawProp := range s.Properties {
		var p struct {
			Type  string `json:"type"`
			Enum  []any  `json:"enum"`
			OneOf []struct {
				Const any `json:"const"`
			} `json:"oneOf"`
			AnyOf []struct {
				Const any `json:"const"`
			} `json:"anyOf"`
		}
		if err := json.Unmarshal(rawProp, &p); err != nil {
			return nil, fmt.Errorf("property %q is not an object", name)
		}
		switch p.Type {
		case "string", "number", "integer", "boolean":
		default:
			return nil, fmt.Errorf("property %q has type %q; only flat properties are supported", name, p.Type)
		}
		prop := formProperty{typ: p.Type, enum: p.Enum}
		for _, v := range append(p.OneOf, p.AnyOf...) {
			prop.enum = append(prop.enum, v.Const)
		}
		fs.props[name] = prop
	}
	for _, r := range s.Required {
		if _, ok := fs.props[r]; !ok {
			return nil, fmt.Errorf("required property %q is not defined", r)
		}
	}
	return fs, nil
}

// check validates an answer: an object of at most 64 KiB, with no extra
// keys, every required key present, and values of the declared types and
// enums.
func (fs *formSchema) check(content json.RawMessage) error {
	if len(content) > maxFormContentBytes {
		return fmt.Errorf("answer is larger than %d bytes", maxFormContentBytes)
	}
	var obj map[string]any
	if err := json.Unmarshal(content, &obj); err != nil || obj == nil {
		return errors.New("answer is not a JSON object")
	}
	var problems []string
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		prop, ok := fs.props[k]
		if !ok {
			problems = append(problems, fmt.Sprintf("field %q is not in the form", k))
			continue
		}
		if msg := prop.check(obj[k]); msg != "" {
			problems = append(problems, fmt.Sprintf("field %q %s", k, msg))
		}
	}
	for _, r := range fs.required {
		if _, ok := obj[r]; !ok {
			problems = append(problems, fmt.Sprintf("field %q is required", r))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (p formProperty) check(v any) string {
	switch p.typ {
	case "string":
		if _, ok := v.(string); !ok {
			return "must be a string"
		}
	case "number":
		if _, ok := v.(float64); !ok {
			return "must be a number"
		}
	case "integer":
		f, ok := v.(float64)
		if !ok || f != math.Trunc(f) {
			return "must be an integer"
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return "must be true or false"
		}
	}
	if p.enum != nil {
		if slices.Contains(p.enum, v) {
			return ""
		}
		return "has a value that is not one of the allowed values"
	}
	return ""
}
