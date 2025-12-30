package main

import (
    "fmt"
    "github.com/sjwhitworth/golearn/base"
    "github.com/sjwhitworth/golearn/evaluation"
    "github.com/sjwhitworth/golearn/knn"
    adapters "github.com/wdm0006/janitor/adapters/golearn"
    csvio "github.com/wdm0006/janitor/pkg/io/csvio"
    "github.com/wdm0006/janitor/imputation"
)

func main() {
    // Load CSV via new csvio reader and convert to golearn instances for this example
    rdr, f, err := csvio.Open("examples/data/iris_nulls.csv", csvio.ReaderOptions{HasHeader: false, SampleRows: 10})
    if err != nil { panic(err) }
    defer func() { _ = f.Close() }()
    schema, _, err := rdr.InferSchema()
    if err != nil { panic(err) }
    fr, err := rdr.ReadAll(schema)
    if err != nil { panic(err) }
    rawData, err := adapters.ToDenseInstances(fr)
    if err != nil { panic(err) }
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
    if err := cls.Fit(trainData); err != nil { panic(err) }

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
