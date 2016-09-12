
//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//Load the CSV file into a RDD
val autoData = sc.textFile("auto-miles-per-gallon.csv")
autoData.cache()

//Remove the first line (contains headers)
val dataLines = autoData.filter(x => !x.contains( "CYLINDERS"))
dataLines.count()

//Convert the RDD into a Dense Vector. As a part of this exercise
//   1. Remove unwanted columns
//   2. Change non-numeric ( values=? ) to numeric
//Use default for average HP

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

val avgHP =sc.broadcast(80.0)

def transformToNumeric( inputStr : String) : Vector = {

    val attList=inputStr.split(",") 
    //Replace ? values with a normal value
    var hpValue = attList(3)
    if (hpValue.contains("?")) {
        hpValue=avgHP.value.toString
    }
    //Filter out columns not wanted at this stage
    val values= Vectors.dense(attList(0).toFloat, 
                     attList(1).toFloat,  
                     hpValue.toFloat,    
                     attList(5).toFloat,  
                     attList(6).toFloat
                     )
    return values
}
//Keep only MPG, CYLINDERS, HP,ACCELERATION and MODELYEAR
val autoVectors = dataLines.map(transformToNumeric)
autoVectors.collect()

//Perform statistical Analysis
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
val autoStats=Statistics.colStats(autoVectors)
autoStats.mean
autoStats.variance
autoStats.min
autoStats.max

val corValues=Statistics.corr(autoVectors)

//Transform to a Data Frame for input to Machine Learing
//Drop columns that are not required (low correlation)
def transformToLabelVectors(inStr : Vector ) : (Double,Vector) = { 
    val labelVectors = (inStr(0), 
            Vectors.dense(inStr(1),inStr(2),inStr(4)))
    return labelVectors
}
val autoLabelVectors = autoVectors.map(transformToLabelVectors)
    
val autoDF = sqlContext.createDataFrame(autoLabelVectors).toDF("label","features")

autoDF.select("label","features").show(10)

//Split into training and testing data
val Array(trainingData, testData) = autoDF.randomSplit(Array(0.9, 0.1))
trainingData.count()
testData.count()

//Build the model on training data
import org.apache.spark.ml.regression.LinearRegression
val lr = new LinearRegression().setMaxIter(10)
val lrModel = lr.fit(trainingData)

println("Coefficients: " + lrModel.coefficients)
print("Intercept: " + lrModel.intercept)
lrModel.summary.r2

//Predict on the test data
val predictions = lrModel.transform(testData)
predictions.select("prediction","label","features").show()

//Evaluate the results. Find R2
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator = new RegressionEvaluator()
evaluator.setPredictionCol("prediction")
evaluator.setLabelCol("label")
evaluator.setMetricName("r2")
evaluator.evaluate(predictions)


