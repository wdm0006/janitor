package dataio

import (
	"encoding/csv"
	"fmt"
	"github.com/sjwhitworth/golearn/base"
	"io"
	"os"
	"regexp"
	"runtime"
	"strings"
)

// ParseDirtyCSVToInstances reads the CSV file given by filepath and returns
// the read Instances. CSV may have missing or malformed values.
func ParseDirtyCSVToInstances(filepath string, hasHeaders bool, n_samples int) (instances *base.DenseInstances, err error) {

	// Open the file
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the number of rows in the file
	rowCount, err := base.ParseCSVGetRows(filepath)
	if err != nil {
		return nil, err
	}

	if hasHeaders {
		rowCount--
	}

	// Read the row headers
	attrs := ParseCSVGetAttributes(filepath, hasHeaders, n_samples)
	specs := make([]base.AttributeSpec, len(attrs))

	// Allocate the Instances to return
	instances = base.NewDenseInstances()
	for i, a := range attrs {
		spec := instances.AddAttribute(a)
		specs[i] = spec
	}
	instances.Extend(rowCount)
	err = ParseCSVBuildInstancesFromReader(f, attrs, hasHeaders, instances)
	if err != nil {
		return nil, err
	}

	instances.AddClassAttribute(attrs[len(attrs)-1])

	return instances, nil
}

// ParseCSVBuildInstancesFromReader updates an [[#UpdatableDataGrid]] from a io.Reader
func ParseCSVBuildInstancesFromReader(r io.Reader, attrs []base.Attribute, hasHeader bool, u base.UpdatableDataGrid) (err error) {
	var rowCounter int

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(err)
			}
			err = fmt.Errorf("Error at line %d (error %s)", rowCounter, r.(error))
		}
	}()

	specs := base.ResolveAttributes(u, attrs)
	reader := csv.NewReader(r)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if rowCounter == 0 {
			if hasHeader {
				hasHeader = false
				continue
			}
		}
		for i, v := range record {
			if strings.Compare(strings.TrimSpace(v), "") == 1 {
				parsed_val := base.Attribute.GetSysValFromString(specs[i].GetAttribute(), strings.TrimSpace(v))
				u.Set(specs[i], rowCounter, parsed_val)
			} else {
				if base.Attribute.GetType(specs[i].GetAttribute()) == 1 {
					parsed_val := base.Attribute.GetSysValFromString(specs[i].GetAttribute(), "NaN")
					u.Set(specs[i], rowCounter, parsed_val)
				} else {
					parsed_val := base.Attribute.GetSysValFromString(specs[i].GetAttribute(), "")
					u.Set(specs[i], rowCounter, parsed_val)
				}

			}

		}
		rowCounter++
	}

	return nil
}

// ParseCSVGetAttributes returns an ordered slice of appropriate-ly typed
// and named Attributes.
func ParseCSVGetAttributes(filepath string, hasHeaders bool, n_samples int) []base.Attribute {
	attrs := ParseCSVSniffAttributeTypes(filepath, hasHeaders, n_samples)
	names := base.ParseCSVSniffAttributeNames(filepath, hasHeaders)
	for i, attr := range attrs {
		attr.SetName(names[i])
	}
	return attrs
}

// ParseCSVSniffAttributeTypes returns a slice of appropriately-typed Attributes.
//
// The type of a given attribute is determined by looking at the first data row
// of the CSV.
func ParseCSVSniffAttributeTypes(filepath string, hasHeaders bool, n_samples int) []base.Attribute {
	var attrs []base.Attribute
	// Open file
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// Create the CSV reader
	reader := csv.NewReader(file)
	if hasHeaders {
		// Skip the headers
		_, err := reader.Read()
		if err != nil {
			panic(err)
		}
	}
	// Read the first line of the file
	columns, err := reader.Read()

	// instantiate our 2d sample array
	columns_list := make([][]string, len(columns))
	for i := range columns_list {
		columns_list[i] = make([]string, n_samples)
	}

	for i := 0; i < n_samples; i++ {
		for idx, entry := range columns {
			columns_list[idx][i] = entry
		}
		columns, err = reader.Read()
	}

	if err != nil {
		panic(err)
	}

	for _, entries := range columns_list {
		// Match the Attribute type with regular expressions
		matched := 0
		didnt_match := 0
		for _, entry := range entries {
			entry = strings.Trim(entry, " ")
			m, err := regexp.MatchString("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$", entry)
			if err != nil {
				panic(err)
			} else {
				if m {
					matched++
				} else {
					didnt_match++
				}
			}
		}

		if matched > didnt_match {
			attrs = append(attrs, base.NewFloatAttribute(""))
		} else {
			attrs = append(attrs, new(base.CategoricalAttribute))
		}
	}

	// Estimate file precision
	maxP, err := base.ParseCSVEstimateFilePrecision(filepath)
	if err != nil {
		panic(err)
	}
	for _, a := range attrs {
		if f, ok := a.(*base.FloatAttribute); ok {
			f.Precision = maxP
		}
	}

	return attrs
}
