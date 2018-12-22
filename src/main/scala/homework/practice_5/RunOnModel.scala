package homework.practice_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

/**
  * @author Aashish Dulal
  */
object RunOnModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "KNVTwiYajvHab5NBqBWSWepde"
    val apiSecret = "62eWYe5bBZlf09mXzpdgtOAzSBm7FxlkoZCgCYmRblpTOSI4wu"
    val accessToken = "863078539347861504-eWeovitnbZ2g8eNinenoMTz1PTfMbOY"
    val accessTokenSecret = "UP1tsTaFuVXTmDJ3nIxGYo0mhqt3ybOuClEQ1V4oTWvbT"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)
    val modelOutput = "data/model-tweets"

    println("Initializing the KMeans model...")
    val model = KMeansModel.load(sc, modelOutput)
    val langNumber = 3
    val filtered = tweets.filter(t => model.predict(TrainUtils.featurize(t)) == langNumber)
    filtered.print()
    ssc.start()
    ssc.awaitTermination()
  }
}