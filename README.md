Janitor
=======

A golearn-compatible data preprocessing library. Very much a WIP. Very much based on golearn.

To start with we're working on loading datasets that have issues with them, and then resolving those issues in a
reasonable way before feeding into golearn. So far that has been loading csvs with missing values for numeric  fields,
and then imputing those missing values with a constant.  In the examples directory, there is an example of doing this
with the iris dataset, where we impute zeros and then make predictions on that data.

Getting Started
===============

We view this as a complement to golearn, so most of the interfaces are the same. To run our version of their KNN example
either run whats in the examples directory, or something like:

```go
package main

import (
	"fmt"
	"github.com/wdm0006/janitor/dataio"
	"github.com/wdm0006/janitor/imputation"
	"github.com/sjwhitworth/golearn/knn"
	"github.com/sjwhitworth/golearn/evaluation"
	"github.com/sjwhitworth/golearn/base"
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
```

```
Reference Class	True Positives	False Positives	True Negatives	Precision	Recall	F1 Score
---------------	--------------	---------------	--------------	---------	------	--------
Iris-virginica	28	           	3		        56		        0.9032		0.9655	0.9333
Iris-versicolor	26		        1		        58		        0.9630		0.8966	0.9286
Iris-setosa	    29		        0		        58		        1.0000		0.9667	0.9831
Overall accuracy: 0.9432
```