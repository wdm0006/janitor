//go:build !parquet

package parquetio

import (
	"errors"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

func WriteAll(path string, f *j.Frame) error {
	return errors.New("parquet support not built; build with -tags parquet and add a parquet implementation")
}
