package homework.practice_1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Aashish Dulal
  */
object CalculateInt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Int Calculation")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val inpitFile = "test.txt"
    val rDDFile = sc.textFile(inpitFile)
    val rows = rDDFile.map(str => str.split(" "))
    val intRows = rows.map(row => row.map(elem => elem.toInt))
    //sum by rows
    val sumRows = intRows.map(arr => arr.sum)
    //calc mod five file
    val sumRowsModFive = intRows.map(arr => arr.filter(value => value % 5 == 0).sum)
    val maxElem = intRows.map(arr => arr.max)
    val minElem = intRows.map(arr => arr.min)
    val distinctElems = intRows.map(arr => arr.distinct)
    println("Sum")
    sumRows.foreach(println)
    println("min:")
    minElem.foreach(println)
    println("max:")
    maxElem.foreach(println)
    println("distinct:")
    distinctElems.map(arr=> arr.mkString(" ")).foreach(println)

    //    println(sumRows.count())
  }


/*
var line = 0
    val intRows = rows.map(row => {
      line += 1; (line, row.map(elem => elem.toInt))
    })
    val sumRows = intRows.map(arrStr => (arrStr._1, arrStr._2.sum))
 */
}
