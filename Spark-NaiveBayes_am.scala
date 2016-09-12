
//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//Load the CSV file into a RDD
val smsData = sc.textFile("SMSSpamCollection.csv",2)
smsData.cache()
smsData.collect()

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

//Transform to Array - to create a data frame with label and message
def transformToLabelVectors(inStr : String  ) : (Double,String) = { 
    val attList=inStr.split(",")
    val smsType = attList(0).contains("spam") match {
            case  true => 1.0
            case  false    => 0.0
     }
    return (smsType, attList(1))
}

//Create a data frame
val smsXformed=smsData.map(transformToLabelVectors)
val smsDf = sqlContext.createDataFrame(smsXformed).toDF("label","message")
smsDf.cache()
smsDf.select("label","message").show(10)

//Split training and testing
val Array(trainingData, testData) = smsDf.randomSplit(Array(0.9, 0.1))
trainingData.count()
testData.count()

//Setup pipeline
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, IDF}

//Setup tokenizer that splits sentences to words
val tokenizer = new Tokenizer()
tokenizer.setInputCol("message")
tokenizer.setOutputCol("words")

//Setup the TF compute function
val hashingTF = new HashingTF()
hashingTF.setInputCol(tokenizer.getOutputCol)
hashingTF.setOutputCol("tempfeatures")

//Setup the IDF compute function
val idf=new IDF()
idf.setInputCol(hashingTF.getOutputCol)
idf.setOutputCol("features")

//Setup the Naive Bayes classifier
val nbClassifier=new NaiveBayes()

//Setup the pipeline with all the transformers
val pipeline = new Pipeline()
pipeline.setStages(Array(tokenizer, hashingTF,idf, nbClassifier))

//Build the model
val nbModel=pipeline.fit(trainingData)

//Predict on the test data
val prediction=nbModel.transform(testData)

//Evaluate the precision of prediction
val evaluator = new MulticlassClassificationEvaluator()
evaluator.setPredictionCol("prediction")
evaluator.setLabelCol("label")
evaluator.setMetricName("precision")
evaluator.evaluate(prediction) 

//Print confusion matrix.
prediction.groupBy("label","prediction").count().show()
