package main

import (
	"fmt"
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/evaluation"
	"github.com/sjwhitworth/golearn/knn"
	"github.com/wdm0006/janitor/dataio"
	"github.com/wdm0006/janitor/imputation"
)

func main() {
	// we use our custom parser to pull back a csv that has some missing values
	rawData, err := dataio.ParseDirtyCSVToInstances("examples/data/iris_nulls.csv", false, 10)
	if err != nil {
		panic(err)
	}
	// printing it out we can see NaN values in float fields and some empty strings
	fmt.Println(rawData)

	// the ConstantImputer will let us specify a default float value for NaNs, shown here.
	imputer := imputation.NewConstantImputer(0.0)
	clean_data := imputer.Transform(rawData)

	// and there we go, kinda clean data.
	fmt.Println(clean_data)

	// Now we can proceed as we normally would with golearn:

	//Initialises a new KNN classifier
	cls := knn.NewKnnClassifier("euclidean", "linear", 2)

	//Do a training-test split
	trainData, testData := base.InstancesTrainTestSplit(rawData, 0.50)
	cls.Fit(trainData)

	//Calculates the Euclidean distance and returns the most popular label
	predictions, err := cls.Predict(testData)
	if err != nil {
		panic(err)
	}
	fmt.Println(predictions)

	// Prints precision/recall metrics
	confusionMat, err := evaluation.GetConfusionMatrix(testData, predictions)
	if err != nil {
		panic(fmt.Sprintf("Unable to get confusion matrix: %s", err.Error()))
	}
	fmt.Println(evaluation.GetSummary(confusionMat))

}
