
//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//Load the CSV file into a RDD
val irisData = sc.textFile("iris.csv")
irisData.cache()

//Remove the first line (contains headers)
val dataLines = irisData.filter(x =>  !x.contains("Sepal"))
dataLines.count()

//Convert the RDD into a Dense Vector. As a part of this exercise
//   1. Change labels to numeric ones

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

def transformToNumeric( inputStr : String) : Vector = {
    val attList=inputStr.split(",")
    
    //Set default to setosa
    var irisValue=1.0
    if (attList(4).contains("versicolor") ){
        irisValue=2.0
    }
    else if (attList(4).contains("virginica") ) {
        irisValue=3.0
    }
    //Filter out columns not wanted at this stage
    val values= Vectors.dense(irisValue, 
                     attList(0).toFloat,  
                     attList(1).toFloat,  
                     attList(2).toFloat,  
                     attList(3).toFloat 
                     )
    return values
 }   
//Change to a Vector
val irisVectors = dataLines.map(transformToNumeric)
irisVectors.collect()

//Perform statistical Analysis
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
val irisStats=Statistics.colStats(irisVectors)
irisStats.mean
irisStats.variance
irisStats.min
irisStats.max

Statistics.corr(irisVectors)

//Transform to a Data Frame for input to Machine Learing
//Drop columns that are not required (low correlation)

def transformToLabelVectors(inStr : String  ) : (String,Vector) = { 
    val attList=inStr.split(",")
    val labelVectors  = ( attList(4), 
        Vectors.dense(attList(0).toFloat,attList(2).toFloat,attList(3).toFloat))
    return labelVectors
}    
val irisLabeledVectors = dataLines.map(transformToLabelVectors)
val irisDF = sqlContext.createDataFrame(irisLabeledVectors).toDF("label","features")
irisDF.select("label","features").show(10)
   
//Indexing needed as pre-req for Decision Trees
import org.apache.spark.ml.feature.StringIndexer
val stringIndexer = new StringIndexer()
stringIndexer.setInputCol("label")
stringIndexer.setOutputCol("indexed")
val si_model = stringIndexer.fit(irisDF)
val indexedIris = si_model.transform(irisDF)
indexedIris.select("label","indexed","features").show()
indexedIris.groupBy("label","indexed").count().show()

//Split into training and testing data
val Array(trainingData, testData) = indexedIris.randomSplit(Array(0.9, 0.1))
trainingData.count()
testData.count()

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

//Create the model
val dtClassifier = new DecisionTreeClassifier()
dtClassifier.setMaxDepth(4)
dtClassifier.setLabelCol("indexed")
val dtModel = dtClassifier.fit(trainingData)

dtModel.numNodes
dtModel.depth

//Predict on the test data
val predictions = dtModel.transform(testData)
predictions.select("prediction","indexed","label","features").show()

val evaluator = new MulticlassClassificationEvaluator()
evaluator.setPredictionCol("prediction")
evaluator.setLabelCol("indexed")
evaluator.setMetricName("precision")
evaluator.evaluate(predictions)      

//Draw a confusion matrix
predictions.groupBy("indexed","prediction").count().show()

