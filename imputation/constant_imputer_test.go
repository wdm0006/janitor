package imputation

import (
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/ensemble"
	"github.com/sjwhitworth/golearn/evaluation"
	"github.com/smartystreets/goconvey/convey"
	"github.com/wdm0006/janitor/dataio"
	"testing"
)

func TestConstantImputer(t *testing.T) {
	convey.Convey("Given a valid CSV file", t, func() {
		inst, err := dataio.ParseDirtyCSVToInstances("../examples/data/iris_nulls.csv", true, 5)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Try Imputing some data", func() {
			imputer := NewConstantImputer(0.0)
			clean_data := imputer.Transform(inst)

			convey.Convey("Splitting the data into test and training sets", func() {
				trainData, testData := base.InstancesTrainTestSplit(clean_data, 0.60)

				convey.Convey("Fitting and predicting with a Random Forest", func() {
					rf := ensemble.NewRandomForest(10, 3)
					err = rf.Fit(trainData)
					convey.So(err, convey.ShouldBeNil)

					predictions, err := rf.Predict(testData)
					convey.So(err, convey.ShouldBeNil)

					confusionMat, err := evaluation.GetConfusionMatrix(testData, predictions)
					convey.So(err, convey.ShouldBeNil)

					convey.Convey("Predictions should be somewhat accurate", func() {
						convey.So(evaluation.GetAccuracy(confusionMat), convey.ShouldBeGreaterThan, 0.35)
					})
				})
			})
		})
	})
}
