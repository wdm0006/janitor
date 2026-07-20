package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

func TestParseConfigFormatParity(t *testing.T) {
	configs := map[string]string{
		"rules.json": `{"input":{"path":"in.csv","type":"csv","has_header":true},"output":{"path":"out.csv","type":"csv"},"steps":[{"trim":{"column":"name"}},{"lower":{"column":"name"}}]}`,
		"rules.yaml": "input:\n  path: in.csv\n  type: csv\n  has_header: true\noutput:\n  path: out.csv\n  type: csv\nsteps:\n  - trim:\n      column: name\n  - lower:\n      column: name\n",
		"rules.toml": "[input]\npath = 'in.csv'\ntype = 'csv'\nhas_header = true\n[output]\npath = 'out.csv'\ntype = 'csv'\n[[steps]]\n[steps.trim]\ncolumn = 'name'\n[[steps]]\n[steps.lower]\ncolumn = 'name'\n",
	}

	var want Config
	if err := parseConfig("rules.json", []byte(configs["rules.json"]), &want); err != nil {
		t.Fatal(err)
	}
	for name, contents := range configs {
		t.Run(name, func(t *testing.T) {
			var got Config
			if err := parseConfig(name, []byte(contents), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("parseConfig() = %#v, want %#v", got, want)
			}
		})
	}
}

func TestParseConfigRejectsMalformedSteps(t *testing.T) {
	for name, contents := range map[string]string{
		"not an object":      `{"steps":["trim"]}`,
		"multiple keys":      `{"steps":[{"trim":{"column":"name"},"lower":{"column":"name"}}]}`,
		"invalid parameters": `{"steps":[{"trim":"name"}]}`,
		"null parameters":    `{"steps":[{"trim":null}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var cfg Config
			err := parseConfig("rules.json", []byte(contents), &cfg)
			if err == nil || !strings.Contains(err.Error(), "step 1") {
				t.Fatalf("parseConfig() error = %v, want descriptive step error", err)
			}
		})
	}
}

func TestExampleConfigParses(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "examples", "config", "rules.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := parseConfig("rules.yml", b, &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(cfg.Steps))
	}
}

func TestCLIExecutesYAMLAndTOMLConfigs(t *testing.T) {
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "janitor")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}

	input := filepath.Join(tmp, "input.csv")
	if err := os.WriteFile(input, []byte("name\n  Alice  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, format := range []struct {
		name   string
		ext    string
		config func(output string) string
	}{
		{name: "YAML", ext: ".yaml", config: func(output string) string {
			return "input:\n  path: " + input + "\n  type: csv\n  has_header: true\noutput:\n  path: " + output + "\n  type: csv\nsteps:\n  - trim:\n      column: name\n"
		}},
		{name: "TOML", ext: ".toml", config: func(output string) string {
			return "[input]\npath = " + jsonString(input) + "\ntype = 'csv'\nhas_header = true\n[output]\npath = " + jsonString(output) + "\ntype = 'csv'\n[[steps]]\n[steps.trim]\ncolumn = 'name'\n"
		}},
	} {
		t.Run(format.name, func(t *testing.T) {
			output := filepath.Join(tmp, strings.ToLower(format.name)+".csv")
			configPath := filepath.Join(tmp, "rules"+format.ext)
			if err := os.WriteFile(configPath, []byte(format.config(output)), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "--config", configPath)
			if combined, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("run CLI: %v\n%s", err, combined)
			}
			got, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "name\nAlice\n" {
				t.Fatalf("output = %q, want transformed value", got)
			}
		})
	}
}

func jsonString(value string) string {
	b, _ := json.Marshal(value)
	return string(b)
}
