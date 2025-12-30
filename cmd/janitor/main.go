package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	csvio "github.com/wdm0006/janitor/pkg/io/csvio"
	jsonlio "github.com/wdm0006/janitor/pkg/io/jsonlio"
	j "github.com/wdm0006/janitor/pkg/janitor"
	imp "github.com/wdm0006/janitor/pkg/transform/impute"
	outl "github.com/wdm0006/janitor/pkg/transform/outliers"
	std "github.com/wdm0006/janitor/pkg/transform/standardize"
	val "github.com/wdm0006/janitor/pkg/transform/validate"
)

var (
	version = "0.1.0-dev"
)

type Config struct {
	Input struct {
		Path      string `json:"path"`
		Type      string `json:"type"` // csv|jsonl (default csv)
		HasHeader bool   `json:"has_header"`
		Delimiter string `json:"delimiter"`
	} `json:"input"`
	Output struct {
		Path      string `json:"path"`
		Type      string `json:"type"` // csv|jsonl (default csv)
		Delimiter string `json:"delimiter"`
	} `json:"output"`
	Steps []json.RawMessage `json:"steps"`
}

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	configPath := flag.String("config", "", "Path to cleaning config (JSON)")
	chunkSize := flag.Int("chunk-size", 0, "Enable streaming with chunk size (rows per chunk). 0 disables streaming.")
	flag.Parse()

	if *showVersion {
		fmt.Println("janitor", version)
		return
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "no config provided; nothing to do. try --config <file> or --version")
		os.Exit(2)
	}

	b, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var frame *j.Frame
	useStream := *chunkSize > 0
	if !useStream {
		switch cfg.Input.Type {
		case "", "csv":
			delim := ','
			if cfg.Input.Delimiter != "" {
				delim = rune(cfg.Input.Delimiter[0])
			}
			rdr, file, err := csvio.Open(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 100})
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
        defer func() { _ = file.Close() }()
			schema, _, err := rdr.InferSchema()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			frame, err = rdr.ReadAll(schema)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		case "jsonl":
			jr, jf, err := jsonlio.Open(cfg.Input.Path, jsonlio.ReaderOptions{SampleRows: 100})
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
            defer func() { _ = jf.Close() }()
			schema, err := jr.InferSchema()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			frame, err = jr.ReadAll(schema)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
			os.Exit(2)
		}
	}

	p := j.NewPipeline()
	for _, raw := range cfg.Steps {
		// detect each step by its single key
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(raw, &probe); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for k, v := range probe {
			switch k {
			case "impute_constant":
				var s struct {
					Column string `json:"column"`
					Value  any    `json:"value"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&imp.Constant{Column: s.Column, Value: s.Value})
			case "impute_mean":
				var s struct {
					Column string `json:"column"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&imp.Mean{Column: s.Column})
			case "trim":
				var s struct {
					Column string `json:"column"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&std.Trim{Column: s.Column})
			case "lower":
				var s struct {
					Column string `json:"column"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&std.Lower{Column: s.Column})
			case "regex_replace":
				var s struct {
					Column  string `json:"column"`
					Pattern string `json:"pattern"`
					Replace string `json:"replace"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&std.RegexReplace{Column: s.Column, Pattern: s.Pattern, Replace: s.Replace})
			case "map_values":
				var s struct {
					Column string            `json:"column"`
					Map    map[string]string `json:"map"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&std.MapValues{Column: s.Column, Map: s.Map})
			case "impute_median":
				var s struct {
					Column string `json:"column"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&imp.Median{Column: s.Column})
			case "validate_in":
				var s struct {
					Column string   `json:"column"`
					Values []string `json:"values"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(val.NewInSet(s.Column, s.Values))
			case "validate_range":
				var s struct {
					Column string   `json:"column"`
					Min    *float64 `json:"min"`
					Max    *float64 `json:"max"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&val.Range{Column: s.Column, Min: s.Min, Max: s.Max})
			case "cap_range":
				var s struct {
					Column string   `json:"column"`
					Min    *float64 `json:"min"`
					Max    *float64 `json:"max"`
				}
				_ = json.Unmarshal(v, &s)
				p.Add(&outl.Cap{Column: s.Column, Min: s.Min, Max: s.Max})
			default:
				fmt.Fprintf(os.Stderr, "warning: unknown step %q ignored\n", k)
			}
		}
	}

	if useStream {
		// streaming path
		switch cfg.Input.Type {
		case "", "csv":
			delim := ','
			if cfg.Input.Delimiter != "" {
				delim = rune(cfg.Input.Delimiter[0])
			}
			sr, f, err := csvio.NewStreamReader(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 100}, *chunkSize)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
            defer func() { _ = f.Close() }()
			switch cfg.Output.Type {
			case "", "csv":
				outDelim := ','
				if cfg.Output.Delimiter != "" {
					outDelim = rune(cfg.Output.Delimiter[0])
				}
				sw, err := csvio.NewStreamWriter(cfg.Output.Path, sr.Schema(), csvio.WriterOptions{Delimiter: outDelim})
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if err := j.RunStream(context.Background(), p, sr, sw); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			case "jsonl":
				sw, err := jsonlio.NewStreamWriter(cfg.Output.Path)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if err := j.RunStream(context.Background(), p, sr, sw); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			default:
				fmt.Fprintf(os.Stderr, "unsupported output type %q for streaming\n", cfg.Output.Type)
				os.Exit(2)
			}
		case "jsonl":
			sr, f, err := jsonlio.NewStreamReader(cfg.Input.Path, *chunkSize)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
            defer func() { _ = f.Close() }()
			switch cfg.Output.Type {
			case "jsonl":
				sw, err := jsonlio.NewStreamWriter(cfg.Output.Path)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if err := j.RunStream(context.Background(), p, sr, sw); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			case "", "csv":
				outDelim := ','
				if cfg.Output.Delimiter != "" {
					outDelim = rune(cfg.Output.Delimiter[0])
				}
				sw, err := csvio.NewStreamWriter(cfg.Output.Path, sr.Schema(), csvio.WriterOptions{Delimiter: outDelim})
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if err := j.RunStream(context.Background(), p, sr, sw); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			default:
				fmt.Fprintf(os.Stderr, "unsupported output type %q for streaming\n", cfg.Output.Type)
				os.Exit(2)
			}
		default:
			fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
			os.Exit(2)
		}
		return
	}

	// batch path
	outFrame, err := p.Run(context.Background(), frame)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	switch cfg.Output.Type {
	case "", "csv":
		outDelim := ','
		if cfg.Output.Delimiter != "" {
			outDelim = rune(cfg.Output.Delimiter[0])
		}
		if err := csvio.WriteAll(cfg.Output.Path, outFrame, csvio.WriterOptions{Delimiter: outDelim}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "jsonl":
		if err := jsonlio.WriteAll(cfg.Output.Path, outFrame); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unsupported output type %q\n", cfg.Output.Type)
		os.Exit(2)
	}
}
