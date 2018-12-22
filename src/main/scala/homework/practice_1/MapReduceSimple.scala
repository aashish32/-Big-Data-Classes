package homework.practice_1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aashish Dulal
  */
object MapReduceSimple {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Simple Map Reduce")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val inpitFile = "products.csv"
    val rDDFile = sc.textFile(inpitFile)
    val rows = rDDFile.flatMap(str => str.split(" ")).map(word => (word, 1))
    val result = rows.reduceByKey(_ + _)
    result.foreach(map => {
      println(map._1 + " : " + map._2)
    })

  }
}
