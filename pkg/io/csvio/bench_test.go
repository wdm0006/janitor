package csvio

import (
	"path/filepath"
	"testing"
)

func BenchmarkReadIris(b *testing.B) {
	p := filepath.FromSlash("../../../examples/data/iris_nulls.csv")
	for n := 0; n < b.N; n++ {
		r, f, err := Open(p, ReaderOptions{HasHeader: true})
		if err != nil {
			b.Fatal(err)
		}
		schema, _, err := r.InferSchema()
		if err != nil {
			b.Fatal(err)
		}
		fr, err := r.ReadAll(schema)
		if err != nil {
			b.Fatal(err)
		}
		if fr.Rows() == 0 {
			b.Fatal("no rows")
		}
        _ = f.Close()
	}
}
