package dataio

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestConstantImputer(t *testing.T) {
	convey.Convey("Given a valid CSV file", t, func() {
		inst, err := ParseDirtyCSVToInstances("../examples/data/iris_nulls.csv", true, 5)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Try Imputing some data", func() {
			n_cols, n_rows := inst.Size()
			convey.So(n_cols, convey.ShouldEqual, 5)
			convey.So(n_rows, convey.ShouldEqual, 149)
		})
	})
}
