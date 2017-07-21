package imputation

import (
	"bytes"
	"github.com/sjwhitworth/golearn/base"
)

type ConstantImputer struct {
	impute_val float64
}

// The float value to impute for any missing values
func NewConstantImputer(impute_val float64) *ConstantImputer {
	return &ConstantImputer{impute_val: impute_val}
}

func (imputer *ConstantImputer) Transform(X *base.DenseInstances) *base.DenseInstances {
	sys_nan := []byte{}

	asv := []base.AttributeSpec{}
	for _, attr := range X.AllAttributes() {
		// if its a float, we can impute!
		if attr.GetType() == 1 {
			spec, err := X.GetAttribute(attr)
			if err != nil {
				panic("error retrieving AttributeSpec in Imputer")
			}

			if len(sys_nan) == 0 {
				sys_nan = base.Attribute.GetSysValFromString(attr, "NaN")
			}
			asv = append(asv, spec)
		}
	}

	X.MapOverRows(asv, func(val [][]byte, row int) (bool, error) {
		for col_id, v := range val {
			if bytes.Compare(v, sys_nan) == 0 {
				X.Set(asv[col_id], row, base.PackFloatToBytes(imputer.impute_val))
			}
		}
		return true, nil
	})

	return X
}
