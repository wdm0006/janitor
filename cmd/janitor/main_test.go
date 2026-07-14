package main

import (
	"strings"
	"testing"
)

func TestParseDelimiter(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		defaultVal rune
		want       rune
	}{
		{name: "ASCII", value: ";", defaultVal: ',', want: ';'},
		{name: "non-ASCII", value: "§", defaultVal: ',', want: '§'},
		{name: "empty uses reader default", defaultVal: rune(0), want: rune(0)},
		{name: "empty uses writer default", defaultVal: ',', want: ','},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDelimiter(tt.value, tt.defaultVal)
			if err != nil {
				t.Fatalf("parseDelimiter() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseDelimiter() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseDelimiterRejectsMultipleRunes(t *testing.T) {
	_, err := parseDelimiter("||", ',')
	if err == nil {
		t.Fatal("parseDelimiter() expected an error")
	}
	if !strings.Contains(err.Error(), "exactly one Unicode rune") {
		t.Errorf("parseDelimiter() error = %q, want a clear single-rune requirement", err)
	}
}
