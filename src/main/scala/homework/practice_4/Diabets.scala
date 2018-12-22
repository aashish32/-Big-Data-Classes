package homework.practice_4

/**
  * @author Aashish Dulal
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import scala.collection.mutable

object Diabetes {

  def main(args: Array[String]): Unit = {

    //disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFile = "data/practice_4/diabets.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate();


    //Read CSV file to DF and define scheme on the fly
    val patients = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)

    patients.show(100)
    patients.printSchema()

    //Feature Extraction
    val DFAssembler = new VectorAssembler().
      setInputCols(Array(
        "pregnancy", "glucose", "arterial pressure",
        "thickness of TC", "insulin", "body mass index",
        "heredity", "age")).
      setOutputCol("features")

    val features = DFAssembler.transform(patients)
    features.show(100)

    val labeledFeatures = new StringIndexer().setInputCol("diabet").setOutputCol("label")
    val df3 = labeledFeatures.fit(features).transform(features)
    df3.show(100)

    // Split data into training (60%) and test (40%)
    val splits = df3.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.5)

    //Train Model
    val model = lr.fit(trainingData)

    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    //Make predictions on test data
    val predictions = model.transform(testData)

    predictions.show(100)

    //Evaluate the precision and recall
    val countProve = predictions.where("label == prediction").count()
    val count = predictions.count()

    println(s"Count of true predictions: $countProve Total Count: $count")

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(predictions)

    println(s"Accuracy = ${accuracy}")

    val predictionAndLabels = predictions.select("rawPrediction", "label")
      .rdd.map(x => (x(0).asInstanceOf[DenseVector](1), x(1).asInstanceOf[Double]))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    println("Area under the precision-recall curve: " + metrics.areaUnderPR)

    println("Area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)
  }
}
