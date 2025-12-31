//go:build yaml

package main

import (
    yaml "gopkg.in/yaml.v3"
)

func init() {
    yamlUnmarshal = func(b []byte, v any) error { return yaml.Unmarshal(b, v) }
}

