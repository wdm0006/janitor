package csvio

import (
    "encoding/csv"
    "io"
    "os"
    "bufio"
    "regexp"
    "strconv"
    "strings"

    j "github.com/wdm0006/janitor/pkg/janitor"
    iox "github.com/wdm0006/janitor/pkg/io/ioutils"
    "fmt"
)

type ReaderOptions struct {
    HasHeader  bool
    Delimiter  rune // 0 = sniff, default ','
    SampleRows int  // for inference; default 100
    Strict     bool // if true, error on short/long records
}

type Reader struct {
    r   *csv.Reader
    opt ReaderOptions
    buf [][]string
    // repair/warning counters
    shortRecords int
    longRecords  int
}

// Open opens a CSV file and returns a Reader.
func Open(path string, opt ReaderOptions) (*Reader, *os.File, error) {
    var f *os.File
    var err error
    if path != "-" {
        f, err = os.Open(path)
        if err != nil { return nil, nil, err }
    }
    rc, err := iox.OpenMaybeCompressed(path)
    if err != nil { _ = f.Close(); return nil, nil, err }
    rr := csv.NewReader(rc)
    // sniff delimiter if 0
    if opt.Delimiter == 0 {
        if d, lazy, err := sniffDelimiterAndQuotes(path); err == nil && d != 0 {
            rr.Comma = d
            rr.LazyQuotes = lazy
        }
    } else {
        rr.Comma = opt.Delimiter
    }
    rr.ReuseRecord = true
    return &Reader{r: rr, opt: opt}, f, nil
}

// NewReaderFrom constructs a Reader from an arbitrary io.Reader (stdin, pipe).
func NewReaderFrom(r io.Reader, opt ReaderOptions) *Reader {
    rr := csv.NewReader(r)
    if opt.Delimiter != 0 { rr.Comma = opt.Delimiter }
    rr.ReuseRecord = true
    return &Reader{r: rr, opt: opt}
}

// InferSchema reads header (if present) and samples rows to determine column kinds.
func (r *Reader) InferSchema() (j.Schema, []string, error) {
	var names []string
	// Peek first record to get column count and optionally header
	rec, err := r.r.Read()
	if err != nil {
		return j.Schema{}, nil, err
	}
    if r.opt.HasHeader {
        names = make([]string, len(rec))
        for i := range rec {
            names[i] = strings.ToValidUTF8(rec[i], "?")
        }
        // strip BOM on first header cell if present
        if len(names) > 0 && len(names[0]) > 0 {
            names[0] = strings.TrimPrefix(names[0], "\ufeff")
        }
        rec, err = r.r.Read()
        if err != nil {
            return j.Schema{}, nil, err
        }
    } else {
		names = make([]string, len(rec))
		for i := range names {
			names[i] = "col_" + strconv.Itoa(i)
		}
	}

	sample := [][]string{rec}
	max := r.opt.SampleRows
	if max <= 0 {
		max = 100
	}
	for i := 1; i < max; i++ {
		rr, err := r.r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return j.Schema{}, nil, err
		}
		sample = append(sample, rr)
	}

	kinds := inferKinds(sample)
	schema := j.Schema{Columns: make([]j.ColumnSchema, len(names))}
	for i := range names {
		schema.Columns[i] = j.ColumnSchema{Name: names[i], Type: kinds[i], Nullable: true}
	}
	// retain sampled rows for subsequent ReadAll
	r.buf = append(r.buf, sample...)
	return schema, names, nil
}

// ReadAll loads the rest of the CSV into a Frame.
func (r *Reader) ReadAll(schema j.Schema) (*j.Frame, error) {
    f := j.NewFrame(schema)
    // drain buffered records from inference (if any)
    for len(r.buf) > 0 {
        rec := r.buf[0]
        r.buf = r.buf[1:]
        f.AppendNullRow()
        row := f.Rows() - 1
        for i, cs := range schema.Columns {
            if i >= len(rec) {
                r.shortRecords++
                if r.opt.Strict { return nil, fmt.Errorf("csv short record at buffered read: need %d fields, got %d", len(schema.Columns), len(rec)) }
                continue
            }
            val := strings.ToValidUTF8(strings.TrimSpace(rec[i]), "?")
            if val == "" {
                continue
            }
            switch cs.Type {
            case j.KindFloat:
				if x, err := strconv.ParseFloat(val, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindInt:
				if x, err := strconv.ParseInt(val, 10, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindBool:
				if x, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			default:
				_ = f.SetCell(row, cs.Name, val)
			}
		}
    }
    for {
        rec, err := r.r.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        // append a null row then set non-empty values
        f.AppendNullRow()
        row := f.Rows() - 1
        if len(rec) > len(schema.Columns) {
            r.longRecords++
            if r.opt.Strict { return nil, fmt.Errorf("csv long record at row: need %d fields, got %d", len(schema.Columns), len(rec)) }
        }
        for i, cs := range schema.Columns {
            if i >= len(rec) {
                r.shortRecords++
                if r.opt.Strict { return nil, fmt.Errorf("csv short record at row: need %d fields, got %d", len(schema.Columns), len(rec)) }
                continue
            }
            val := strings.ToValidUTF8(strings.TrimSpace(rec[i]), "?")
            if val == "" {
                continue
            }
            switch cs.Type {
            case j.KindFloat:
				if x, err := strconv.ParseFloat(val, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindInt:
				if x, err := strconv.ParseInt(val, 10, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindBool:
				if x, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			default:
				_ = f.SetCell(row, cs.Name, val)
			}
		}
	}
    return f, nil
}

func inferKinds(rows [][]string) []j.Kind {
	if len(rows) == 0 {
		return nil
	}
	ncol := len(rows[0])
	kinds := make([]j.Kind, ncol)
	// numeric regex similar to old code
	numre := regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	for c := 0; c < ncol; c++ {
		num, integer, str := 0, 0, 0
		for _, row := range rows {
			if c >= len(row) {
				continue
			}
			v := strings.TrimSpace(row[c])
			if v == "" {
				continue
			}
			if numre.MatchString(v) {
				num++
				if !strings.ContainsAny(v, ".eE") {
					integer++
				}
			} else {
				// try bool
				lv := strings.ToLower(v)
				if lv == "true" || lv == "false" {
					continue
				}
				str++
			}
		}
		// prefer float over int to be permissive
		if num > str {
			if integer == num {
				kinds[c] = j.KindInt
			} else {
				kinds[c] = j.KindFloat
			}
		} else {
			kinds[c] = j.KindString
		}
	}
	return kinds
}

func sniffDelimiterAndQuotes(path string) (rune, bool, error) {
    rc, err := iox.OpenMaybeCompressed(path)
    if err != nil { return 0, false, err }
    defer func() { _ = rc.Close() }()
    br := bufio.NewReader(rc)
    sample, _ := br.Peek(4096)
    if len(sample) == 0 { return ',', false, nil }
    candidates := []byte{',', '\t', ';', '|'}
    best := byte(',')
    bestCount := -1
    for _, c := range candidates {
        cnt := 0
        for _, b := range sample {
            if b == c { cnt++ }
        }
        if cnt > bestCount { bestCount = cnt; best = c }
    }
    // naive quote heuristic: if there are many quotes or odd counts, enable LazyQuotes
    quoteCount := 0
    for _, b := range sample { if b == '"' { quoteCount++ } }
    lazy := quoteCount%2 != 0 || quoteCount > 0
    return rune(best), lazy, nil
}

// Warnings returns a summary string of any repairs/mismatches encountered.
func (r *Reader) Warnings() string {
    if r.shortRecords == 0 && r.longRecords == 0 { return "" }
    parts := []string{}
    if r.shortRecords > 0 { parts = append(parts, fmt.Sprintf("short_records=%d", r.shortRecords)) }
    if r.longRecords > 0 { parts = append(parts, fmt.Sprintf("long_records=%d", r.longRecords)) }
    return strings.Join(parts, ", ")
}
