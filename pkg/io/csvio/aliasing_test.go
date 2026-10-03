package csvio

import (
	"io"
	"strconv"
	"strings"
	"testing"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

// aliasCSV has n data rows: id=i, tag=t<i>, mixed is non-numeric except the last row.
func aliasCSV(n int) string {
	var sb strings.Builder
	sb.WriteString("id,tag,mixed\n")
	for i := 1; i <= n; i++ {
		m := "w" + strconv.Itoa(i)
		if i == n {
			m = strconv.Itoa(i)
		}
		sb.WriteString(strconv.Itoa(i) + ",t" + strconv.Itoa(i) + "," + m + "\n")
	}
	return sb.String()
}

func cellStr(t *testing.T, fr *j.Frame, col string, row int) string {
	t.Helper()
	c, ok := fr.ColumnByName(col)
	if !ok {
		t.Fatalf("missing column %s", col)
	}
	switch cc := c.(type) {
	case *j.StringColumn:
		v, _ := cc.Get(row)
		return v
	case *j.IntColumn:
		v, _ := cc.Get(row)
		return strconv.FormatInt(v, 10)
	}
	t.Fatalf("unexpected column type %T", c)
	return ""
}

func checkRows(t *testing.T, fr *j.Frame, offset int, rows []int) {
	t.Helper()
	for _, r := range rows {
		id := offset + r + 1
		if got := cellStr(t, fr, "id", r); got != strconv.Itoa(id) {
			t.Errorf("row %d id = %q, want %d", r, got, id)
		}
		if got := cellStr(t, fr, "tag", r); got != "t"+strconv.Itoa(id) {
			t.Errorf("row %d tag = %q, want t%d", r, got, id)
		}
	}
}

func TestOpenSampleRowsNotAliased(t *testing.T) {
	const n = 10
	for _, sample := range []int{4, 100} {
		p := writeTempCSV(t, aliasCSV(n))
		r, f, err := Open(p, ReaderOptions{HasHeader: true, SampleRows: sample})
		if err != nil {
			t.Fatal(err)
		}
		schema, _, err := r.InferSchema()
		if err != nil {
			t.Fatal(err)
		}
		fr, err := r.ReadAll(schema)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if fr.Rows() != n {
			t.Fatalf("sample=%d rows = %d, want %d", sample, fr.Rows(), n)
		}
		checkRows(t, fr, 0, []int{0, 1, n / 2, n - 1})
	}
}

func TestInferKindsUseAllSampledRows(t *testing.T) {
	p := writeTempCSV(t, aliasCSV(5))
	r, f, err := Open(p, ReaderOptions{HasHeader: true, SampleRows: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	schema, _, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	want := []j.Kind{j.KindInt, j.KindString, j.KindString}
	for i, w := range want {
		if schema.Columns[i].Type != w {
			t.Errorf("column %s kind = %v, want %v", schema.Columns[i].Name, schema.Columns[i].Type, w)
		}
	}
}

func TestStreamSampleRowsNotAliased(t *testing.T) {
	const n = 10
	p := writeTempCSV(t, aliasCSV(n))
	sr, f, err := NewStreamReader(p, ReaderOptions{HasHeader: true, SampleRows: 4}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if got := sr.Schema().Columns[2].Type; got != j.KindString {
		t.Errorf("mixed kind = %v, want string", got)
	}
	seen := 0
	for {
		fr, err := sr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		all := make([]int, fr.Rows())
		for i := range all {
			all[i] = i
		}
		checkRows(t, fr, seen, all)
		seen += fr.Rows()
	}
	if seen != n {
		t.Fatalf("streamed %d rows, want %d", seen, n)
	}
}
