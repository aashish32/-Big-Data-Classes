package homework.practice_5

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession

/**
  * @author Aashish Dulal
  */
object TwitterKMeansTrainer {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("spark-tweets-train-kmeans")
      .master("local[*]")
      .getOrCreate()
    val folders = "data/tweets/[0-9]*\\sms/part-00000"
    val modelOutput = "data/model-tweets"

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(folders)

    val text = tweetsDF.select("text").rdd.map(r => r(0).toString)
    //Get the features vector
    val features = text.map(s => TrainUtils.featurize(s))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val modelRes: KMeansModel = KMeans.train(features, numClusters, numIterations)

    modelRes.save(sparkSession.sparkContext, modelOutput)
  }
}
