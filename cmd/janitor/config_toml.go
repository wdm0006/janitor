//go:build toml

package main

import (
    toml "github.com/pelletier/go-toml/v2"
)

func init() {
    tomlUnmarshal = func(b []byte, v any) error { return toml.Unmarshal(b, v) }
}

