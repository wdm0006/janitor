//go:build parquet

package parquetio

// This file is a placeholder for Parquet support when building with the `parquet` tag.
// Suggested dependency: github.com/parquet-go/parquet-go
// Implementation outline:
//  - Map janitor.Schema to Parquet schema (primitive types only to start)
//  - Create file writer, write rows iterating from Frame
//  - Close writer

import (
	j "github.com/wdm0006/janitor/pkg/janitor"
)

func WriteAll(path string, f *j.Frame) error {
	// TODO: implement using a chosen Parquet library.
	// Kept minimal to avoid adding transitive dependencies without build tag.
	return nil
}
