package homework.practice_3

import org.apache.spark.sql.types._

/**
  * @author Aashish Dulal
  */
object RetailData {
  val dataSchema = StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", StringType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", IntegerType, true),
    StructField("Country", StringType, true)))
}



