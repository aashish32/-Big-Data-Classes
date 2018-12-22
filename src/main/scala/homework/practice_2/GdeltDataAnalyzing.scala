package homework.practice_2

import com.aamend.spark.gdelt.Event
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aashish Dulal
  */
object GdeltDataAnalyzing {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val session = SparkSession.builder()
      .appName("Gdelt analyze")
      .master("local[2]")
      .getOrCreate()
/*
    val inpitFile = "data/second/gdelt.csv"
    val gdeltEventDS: Dataset[Event] = session.read.gdeltEvent(inpitFile)
    val rDDFile = sc.textFile(inpitFile)*/
  }
}
