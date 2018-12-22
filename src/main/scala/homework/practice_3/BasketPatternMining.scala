package homework.practice_3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{SparkSession}

/**
  * @author Aashish Dulal
  */
object BasketPatternMining {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //initialize spark configurations
    val spark = SparkSession
      .builder()
      .appName("market-basket-problem")
      .master("local[*]")
      .getOrCreate()

    val inputFile = "data/practice_3/Online Retail.csv"

    //read file with purchases
    val fileRDD = spark.read
      .option("header", "true")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      //      .schema(RetailData.dataSchema)
      .csv(inputFile)
    fileRDD.show(10)
    fileRDD.createOrReplaceTempView("dataSetTable")
    //get transactions
    case class RetailData(InvoiceNo: String,
                          StockCode: String,
                          Description: String
                         )
    val data = fileRDD.sparkSession.sql(" SELECT InvoiceNo, StockCode" +
      " FROM dataSetTable" +
      " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
    //    val dataForGroth = data.rdd.groupBy(x => x.InvoiceNo).map(x => (x._1, x._2.map(_.StockCode).toArray.distinct))
    val dataForGroth = data.rdd.map(row => (row(0), row(1).toString)).groupByKey().map(x => x._2.toArray.distinct)
    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth()
      .setMinSupport(0.2)

    val model = fpg.run(dataForGroth)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.01
    val rules2 = model.generateAssociationRules(minConfidence)
    val rules = rules2.sortBy(r => r.confidence, ascending = false)

    val dataFrameOfStockCodeAndDescription = fileRDD.sparkSession.sql("SELECT DISTINCT StockCode, Description" +
      " FROM dataSetTable" +
      " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
    val dictionary = dataFrameOfStockCodeAndDescription.rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(s => dictionary(s)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => dictionary(s)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }

  }
}
