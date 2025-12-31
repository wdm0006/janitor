package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
    "time"

    csvio "github.com/wdm0006/janitor/pkg/io/csvio"
	jsonlio "github.com/wdm0006/janitor/pkg/io/jsonlio"
	parquetio "github.com/wdm0006/janitor/pkg/io/parquetio"
    j "github.com/wdm0006/janitor/pkg/janitor"
    profpkg "github.com/wdm0006/janitor/pkg/profile"
    imp "github.com/wdm0006/janitor/pkg/transform/impute"
    outl "github.com/wdm0006/janitor/pkg/transform/outliers"
    std "github.com/wdm0006/janitor/pkg/transform/standardize"
    val "github.com/wdm0006/janitor/pkg/transform/validate"
    toml "github.com/pelletier/go-toml/v2"
    yaml "gopkg.in/yaml.v3"
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
        CSVStrict bool   `json:"csv_strict"`
    } `json:"input"`
    Output struct {
        Path      string `json:"path"`
        Type      string `json:"type"` // csv|jsonl (default csv)
        Delimiter string `json:"delimiter"`
        PartitionBy []string `json:"partition_by"`
    } `json:"output"`
    Steps []json.RawMessage `json:"steps"`
}

func main() {
    showVersion := flag.Bool("version", false, "Print version and exit")
    configPath := flag.String("config", "", "Path to cleaning config (JSON/YAML/TOML)")
    chunkSize := flag.Int("chunk-size", 0, "Enable streaming with chunk size (rows per chunk). 0 disables streaming.")
    verbose := flag.Bool("verbose", false, "Print progress and a summary")
    prof := flag.Bool("profile", false, "Profile the input: print column stats and exit")
    profTopK := flag.Int("profile-topk", 5, "Top-K frequent values to show for string/time columns")
    profJSON := flag.Bool("profile-json", false, "Emit profile in JSON format")
    expectedRows := flag.Int("expected-rows", 0, "Optional expected total rows for ETA in streaming progress")
    dryRun := flag.Bool("dry-run", false, "Infer schema and print planned steps, without reading/writing data")
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
    if err := parseConfig(*configPath, b, &cfg); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    var frame *j.Frame
    var stepNames []string
	useStream := *chunkSize > 0
	if !useStream {
		switch cfg.Input.Type {
		case "", "csv":
            delim := rune(0)
            if cfg.Input.Delimiter != "" {
                delim = rune(cfg.Input.Delimiter[0])
            }
            rdr, file, err := csvio.Open(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 100, Strict: cfg.Input.CSVStrict})
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if file != nil { defer func() { _ = file.Close() }() }
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
            if *verbose {
                fmt.Fprintf(os.Stderr, "read csv: rows=%d cols=%d from %s\n", frame.Rows(), len(schema.Columns), cfg.Input.Path)
                if w := rdr.Warnings(); w != "" { fmt.Fprintf(os.Stderr, "csv repair summary: %s\n", w) }
            }
		case "jsonl":
            jr, jf, err := jsonlio.Open(cfg.Input.Path, jsonlio.ReaderOptions{SampleRows: 100})
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if jf != nil { defer func() { _ = jf.Close() }() }
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
            if *verbose {
                fmt.Fprintf(os.Stderr, "read jsonl: rows=%d cols=%d from %s\n", frame.Rows(), len(schema.Columns), cfg.Input.Path)
            }
		case "parquet":
			fmt.Fprintln(os.Stderr, "parquet input not yet supported; please use CSV/JSONL input.")
			os.Exit(2)
		default:
			fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
			os.Exit(2)
		}
	}

    // Dry-run: print inferred schema and steps, then exit
    if *dryRun {
        // parse steps to stepNames (without building pipeline)
        for _, raw := range cfg.Steps {
            var probe map[string]json.RawMessage
            _ = json.Unmarshal(raw, &probe)
            for k := range probe { stepNames = append(stepNames, k) }
        }
        switch cfg.Input.Type {
        case "", "csv":
            delim := rune(0)
            if cfg.Input.Delimiter != "" { delim = rune(cfg.Input.Delimiter[0]) }
            rdr, f, err := csvio.Open(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 50})
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            if f != nil { defer func() { _ = f.Close() }() }
            schema, _, err := rdr.InferSchema()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            fmt.Fprintf(os.Stderr, "dry-run schema (csv): %v\nsteps: %v\n", schema, stepNames)
        case "jsonl":
            jr, jf, err := jsonlio.Open(cfg.Input.Path, jsonlio.ReaderOptions{SampleRows: 50})
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            if jf != nil { defer func() { _ = jf.Close() }() }
            schema, err := jr.InferSchema()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            fmt.Fprintf(os.Stderr, "dry-run schema (jsonl): %v\nsteps: %v\n", schema, stepNames)
        case "parquet":
            pr, err := parquetio.OpenReader(cfg.Input.Path, 50)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = pr.Close() }()
            schema := pr.Schema()
            fmt.Fprintf(os.Stderr, "dry-run schema (parquet): %v\nsteps: %v\n", schema, stepNames)
        default:
            fmt.Fprintf(os.Stderr, "unsupported input type %q for dry-run\n", cfg.Input.Type)
            os.Exit(2)
        }
        return
    }

    // Profile-only path
    if *prof {
        if *chunkSize <= 0 { *chunkSize = 10000 }
        switch cfg.Input.Type {
        case "", "csv":
            delim := rune(0)
            if cfg.Input.Delimiter != "" { delim = rune(cfg.Input.Delimiter[0]) }
            sr, f, err := csvio.NewStreamReader(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 200}, *chunkSize)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = f.Close() }()
            // create collector
            col := profpkg.NewCollector(sr.Schema(), *profTopK)
            // iterate
            for {
                fr, err := sr.Next()
                if err == io.EOF { break }
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                col.ConsumeFrame(fr)
            }
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        case "jsonl":
            sr, f, err := jsonlio.NewStreamReader(cfg.Input.Path, *chunkSize)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = f.Close() }()
            col := profpkg.NewCollector(sr.Schema(), *profTopK)
            for {
                fr, err := sr.Next()
                if err == io.EOF { break }
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                col.ConsumeFrame(fr)
            }
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        case "parquet":
            pr, err := parquetio.OpenReader(cfg.Input.Path, 200)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = pr.Close() }()
            fr, err := pr.ReadAll()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            col := profpkg.NewCollector(fr.Schema(), *profTopK)
            col.ConsumeFrame(fr)
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        default:
            fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
            os.Exit(2)
        }
    }

    // Build pipeline from steps
    p := j.NewPipeline()
    for _, raw := range cfg.Steps {
        var probe map[string]json.RawMessage
        if err := json.Unmarshal(raw, &probe); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
        for k, v := range probe {
            switch k {
            case "impute_constant":
                var s struct{ Column string `json:"column"`; Value any `json:"value"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&imp.Constant{Column: s.Column, Value: s.Value})
                stepNames = append(stepNames, "impute_constant:"+s.Column)
            case "impute_mean":
                var s struct{ Column string `json:"column"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&imp.Mean{Column: s.Column})
                stepNames = append(stepNames, "impute_mean:"+s.Column)
            case "trim":
                var s struct{ Column string `json:"column"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&std.Trim{Column: s.Column})
                stepNames = append(stepNames, "trim:"+s.Column)
            case "lower":
                var s struct{ Column string `json:"column"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&std.Lower{Column: s.Column})
                stepNames = append(stepNames, "lower:"+s.Column)
            case "regex_replace":
                var s struct{ Column string `json:"column"`; Pattern string `json:"pattern"`; Replace string `json:"replace"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&std.RegexReplace{Column: s.Column, Pattern: s.Pattern, Replace: s.Replace})
                stepNames = append(stepNames, "regex_replace:"+s.Column)
            case "map_values":
                var s struct{ Column string `json:"column"`; Map map[string]string `json:"map"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&std.MapValues{Column: s.Column, Map: s.Map})
                stepNames = append(stepNames, "map_values:"+s.Column)
            case "impute_median":
                var s struct{ Column string `json:"column"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&imp.Median{Column: s.Column})
                stepNames = append(stepNames, "impute_median:"+s.Column)
            case "validate_in":
                var s struct{ Column string `json:"column"`; Values []string `json:"values"` }
                _ = json.Unmarshal(v, &s)
                p.Add(val.NewInSet(s.Column, s.Values))
                stepNames = append(stepNames, "validate_in:"+s.Column)
            case "validate_range":
                var s struct{ Column string `json:"column"`; Min *float64 `json:"min"`; Max *float64 `json:"max"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&val.Range{Column: s.Column, Min: s.Min, Max: s.Max})
                stepNames = append(stepNames, "validate_range:"+s.Column)
            case "cap_range":
                var s struct{ Column string `json:"column"`; Min *float64 `json:"min"`; Max *float64 `json:"max"` }
                _ = json.Unmarshal(v, &s)
                p.Add(&outl.Cap{Column: s.Column, Min: s.Min, Max: s.Max})
                stepNames = append(stepNames, "cap_range:"+s.Column)
            default:
                fmt.Fprintf(os.Stderr, "warning: unknown step %q ignored\n", k)
            }
        }
    }

    if useStream {
        // streaming path
        switch cfg.Input.Type {
        case "", "csv":
            delim := rune(0)
            if cfg.Input.Delimiter != "" { delim = rune(cfg.Input.Delimiter[0]) }
            // expand globs
            paths := []string{cfg.Input.Path}
            if hasWildcards(cfg.Input.Path) {
                matches, _ := filepath.Glob(cfg.Input.Path)
                if len(matches) == 0 { fmt.Fprintln(os.Stderr, "no files matched input path pattern"); os.Exit(2) }
                paths = matches
            }
            if len(paths) > 1 && !strings.Contains(cfg.Output.Path, "{basename}") {
                fmt.Fprintln(os.Stderr, "multiple input files require output.path to include {basename} placeholder")
                os.Exit(2)
            }
            for _, in := range paths {
                sr, f, err := csvio.NewStreamReader(in, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: delim, SampleRows: 100, Strict: cfg.Input.CSVStrict}, *chunkSize)
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                if f != nil { defer func() { _ = f.Close() }() }
                switch cfg.Output.Type {
                case "", "csv":
                    outDelim := ','
                    if cfg.Output.Delimiter != "" { outDelim = rune(cfg.Output.Delimiter[0]) }
                    outPath := cfg.Output.Path
                    if strings.Contains(outPath, "{basename}") {
                        base := filepath.Base(in)
                        outPath = strings.ReplaceAll(outPath, "{basename}", strings.TrimSuffix(base, filepath.Ext(base)))
                    }
                    if len(cfg.Output.PartitionBy) > 0 {
                        makeSink := func(path string, schema j.Schema) (j.ChunkSink, error) {
                            return csvio.NewStreamWriter(path, schema, csvio.WriterOptions{Delimiter: outDelim})
                        }
                        if err := runStreamPartitioned(context.Background(), p, sr, outPath, makeSink, sr.Schema(), cfg.Output.PartitionBy, *verbose); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    } else {
                        sw, err := csvio.NewStreamWriter(outPath, sr.Schema(), csvio.WriterOptions{Delimiter: outDelim})
                        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    }
                case "jsonl":
                    outPath := cfg.Output.Path
                    if strings.Contains(outPath, "{basename}") {
                        base := filepath.Base(in)
                        outPath = strings.ReplaceAll(outPath, "{basename}", strings.TrimSuffix(base, filepath.Ext(base)))
                    }
                    if len(cfg.Output.PartitionBy) > 0 {
                        makeSink := func(path string, schema j.Schema) (j.ChunkSink, error) { return jsonlio.NewStreamWriter(path) }
                        if err := runStreamPartitioned(context.Background(), p, sr, outPath, makeSink, sr.Schema(), cfg.Output.PartitionBy, *verbose); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    } else {
                        sw, err := jsonlio.NewStreamWriter(outPath)
                        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    }
                default:
                    fmt.Fprintf(os.Stderr, "unsupported output type %q for streaming\n", cfg.Output.Type)
                    os.Exit(2)
                }
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
                if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
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
                if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
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
    case "parquet":
        if err := parquetio.WriteAll(cfg.Output.Path, outFrame); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    default:
        fmt.Fprintf(os.Stderr, "unsupported output type %q\n", cfg.Output.Type)
        os.Exit(2)
    }
    if *verbose {
        fmt.Fprintf(os.Stderr, "batch complete: rows=%d cols=%d steps=%v -> %s\n", outFrame.Rows(), len(outFrame.Schema().Columns), stepNames, cfg.Output.Path)
    }
}

// parseConfig detects format from extension (.json, .yaml/.yml, .toml) and unmarshals into cfg.
func parseConfig(path string, b []byte, cfg *Config) error {
    ext := strings.ToLower(filepath.Ext(path))
    switch ext {
    case ".json", "":
        return json.Unmarshal(b, cfg)
    case ".yaml", ".yml":
        return yaml.Unmarshal(b, cfg)
    case ".toml":
        return toml.Unmarshal(b, cfg)
    default:
        return json.Unmarshal(b, cfg)
    }
}

// runStreamWithProgress processes chunks and prints periodic progress when verbose.
func runStreamWithProgress(ctx context.Context, p *j.Pipeline, src j.ChunkSource, sink j.ChunkSink, verbose bool, expected int) error {
    if !verbose { return j.RunStream(ctx, p, src, sink) }
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    done := make(chan error, 1)
    var rows int
    start := time.Now()
    var rates []float64
    go func() {
        for {
            f, err := src.Next()
            if err == io.EOF { done <- nil; return }
            if err != nil { done <- err; return }
            rows += f.Rows()
            out, err := p.Run(ctx, f)
            if err != nil { done <- err; return }
            if err := sink.Write(out); err != nil { done <- err; return }
        }
    }()
    for {
        select {
        case err := <-done:
            return err
        case <-ticker.C:
            elapsed := time.Since(start).Seconds()
            instRate := float64(rows) / (elapsed + 1e-9)
            rates = append(rates, instRate)
            if len(rates) > 5 { rates = rates[len(rates)-5:] }
            var sum float64
            for _, r := range rates { sum += r }
            rate := sum / float64(len(rates))
            if expected > 0 {
                remaining := expected - rows
                if remaining < 0 { remaining = 0 }
                eta := time.Duration(float64(remaining)/(rate+1e-9)) * time.Second
                pct := float64(rows) / float64(expected)
                if pct > 1 { pct = 1 }
                width := 30
                filled := int(pct * float64(width))
                bar := strings.Repeat("=", filled) + strings.Repeat(" ", width-filled)
                fmt.Fprintf(os.Stderr, "[%s] %5.1f%% rows=%d/%d (%.1f r/s) ETA=%s\n", bar, pct*100, rows, expected, rate, eta.Truncate(time.Second))
            } else {
                fmt.Fprintf(os.Stderr, "processed rows=%d (%.1f rows/s) ...\n", rows, rate)
            }
        }
    }
}

// runStreamPartitioned applies p to each chunk from src, splits rows by partition columns,
// and writes each partition to a sink keyed by the expanded outPath template. outPath must
// include placeholders like {col:Name} which will be replaced with the row's column value.
func runStreamPartitioned(ctx context.Context, p *j.Pipeline, src j.ChunkSource, outPath string, makeSink func(path string, schema j.Schema) (j.ChunkSink, error), schema j.Schema, partCols []string, verbose bool) error {
    sinks := map[string]j.ChunkSink{}
    closeAll := func() {
        for _, s := range sinks { _ = s.Close() }
    }
    defer closeAll()
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    done := make(chan error, 1)
    var rows int
    start := time.Now()
    go func() {
        for {
            f, err := src.Next()
            if err == io.EOF { done <- nil; return }
            if err != nil { done <- err; return }
            out, err := p.Run(ctx, f)
            if err != nil { done <- err; return }
            // split by partition keys
            parts := splitFrameByPartitions(out, partCols)
            for key, pf := range parts {
                path := expandOutPath(outPath, schema, partCols, pf, key)
                s, ok := sinks[path]
                if !ok {
                    ss, err := makeSink(path, schema)
                    if err != nil { done <- err; return }
                    sinks[path] = ss
                    s = ss
                }
                if err := s.Write(pf); err != nil { done <- err; return }
                rows += pf.Rows()
            }
        }
    }()
    if !verbose {
        return <-done
    }
    for {
        select {
        case err := <-done:
            return err
        case <-ticker.C:
            elapsed := time.Since(start).Seconds()
            rate := float64(rows) / (elapsed + 1e-9)
            fmt.Fprintf(os.Stderr, "processed rows=%d (%.1f rows/s) ...\n", rows, rate)
        }
    }
}

func splitFrameByPartitions(f *j.Frame, cols []string) map[string]*j.Frame {
    res := map[string]*j.Frame{}
    for r := 0; r < f.Rows(); r++ {
        key := partitionKeyForRow(f, r, cols)
        pf, ok := res[key]
        if !ok {
            pf = j.NewFrame(f.Schema())
            res[key] = pf
        }
        pf.AppendNullRow()
        row := pf.Rows() - 1
        // copy all columns
        for _, cs := range f.Schema().Columns {
            col, _ := f.ColumnByName(cs.Name)
            switch cs.Type {
            case j.KindFloat:
                if v, ok := col.(*j.FloatColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindInt:
                if v, ok := col.(*j.IntColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindBool:
                if v, ok := col.(*j.BoolColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindString:
                if v, ok := col.(*j.StringColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindTime:
                if v, ok := col.(*j.TimeColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            }
        }
    }
    return res
}

func partitionKeyForRow(f *j.Frame, r int, cols []string) string {
    parts := make([]string, len(cols))
    for i, name := range cols {
        col, _ := f.ColumnByName(name)
        switch col := col.(type) {
        case *j.StringColumn:
            if v, ok := col.Get(r); ok { parts[i] = sanitizePartition(v) } else { parts[i] = "_null" }
        case *j.FloatColumn:
            if v, ok := col.Get(r); ok { parts[i] = fmt.Sprintf("%.6g", v) } else { parts[i] = "_null" }
        case *j.IntColumn:
            if v, ok := col.Get(r); ok { parts[i] = fmt.Sprintf("%d", v) } else { parts[i] = "_null" }
        case *j.BoolColumn:
            if v, ok := col.Get(r); ok { if v { parts[i] = "true" } else { parts[i] = "false" } } else { parts[i] = "_null" }
        case *j.TimeColumn:
            if v, ok := col.Get(r); ok { parts[i] = sanitizePartition(v.Format("2006-01-02")) } else { parts[i] = "_null" }
        default:
            parts[i] = "_"
        }
    }
    return strings.Join(parts, "/")
}

func sanitizePartition(s string) string {
    // replace path separators and spaces
    s = strings.ReplaceAll(s, "/", "-")
    s = strings.ReplaceAll(s, "\\", "-")
    s = strings.ReplaceAll(s, " ", "_")
    return s
}

func expandOutPath(tmpl string, schema j.Schema, cols []string, f *j.Frame, key string) string {
    out := tmpl
    // Support {basename} (handled earlier for CSV/JSONL multi-file) and {col:Name}
    for _, name := range cols {
        placeholder := "{col:" + name + "}"
        if strings.Contains(out, placeholder) {
            // compute value from the first row (all rows in partition share it)
            val := ""
            if f.Rows() > 0 {
                col, _ := f.ColumnByName(name)
                switch col := col.(type) {
                case *j.StringColumn:
                    if v, ok := col.Get(0); ok { val = sanitizePartition(v) }
                case *j.FloatColumn:
                    if v, ok := col.Get(0); ok { val = fmt.Sprintf("%.6g", v) }
                case *j.IntColumn:
                    if v, ok := col.Get(0); ok { val = fmt.Sprintf("%d", v) }
                case *j.BoolColumn:
                    if v, ok := col.Get(0); ok { if v { val = "true" } else { val = "false" } }
                case *j.TimeColumn:
                    if v, ok := col.Get(0); ok { val = sanitizePartition(v.Format("2006-01-02")) }
                }
            }
            out = strings.ReplaceAll(out, placeholder, val)
        }
    }
    // if no {col:} placeholders, fall back to the joined key
    if out == tmpl {
        out = strings.ReplaceAll(out, "{basename}", key)
    }
    return out
}

func hasWildcards(path string) bool {
    return strings.ContainsAny(path, "*?[")
}
