package homework.practice_1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aashish Dulal
  */
object CalcInt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Int Calculation")
    conf.setMaster("local[2]")
   /* Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)*/
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inpitFile = "test.txt"
    val rDDFile = sc.textFile(inpitFile)
    val rows = rDDFile.flatMap(str => str.split(" "))
    val intRows = rows.map(row => row.toInt)
    //sum by rows
    val sumRows = intRows.sum
    //calc mod five file
    val sumRowsModFive = intRows.filter(value => value % 5 == 0).sum
    val maxElem = intRows.max
    val minElem = intRows.min
    val distinctElems = intRows.distinct
    println("Sum" + sumRows)
    println("min:" + minElem)
    println("max:" + maxElem)
    println("distinct:")
    distinctElems.foreach(k=>print(k + " "))
//    sc.saveAsTextFile(

    //    println(sumRows.count())
  }
}
