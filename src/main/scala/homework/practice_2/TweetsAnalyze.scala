package homework.practice_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author Aashish Dulal
  */
object TweetsAnalyze {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf()
    conf.setAppName("Tweets Analyze")
    conf.setMaster("local[2]")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.driver.memory", "1g")

    val sc = new SparkContext(conf)
    val inpitFile = "data/sampletweets.json"
    sc.setLogLevel("ERROR")*/
    // define StructType when read add option
    // Create spark session
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val session = SparkSession.builder()
      .appName("Tweets Analyze")
      .master("local[2]")
      .getOrCreate()

    // Create the DataFrame
    val df = session.read
      .option("nullValue", "")
      .json("data/sampletweets.json")
    //Show schema
    df.printSchema()
    df.createOrReplaceTempView("twitter")
    // Show the Data
    //    df.show()
    df.select("body", "actor.displayName").show()
//    df.select("actor.languages", "count(*)").groupBy("actor.languages")
    //get languages
    df.sqlContext.sql("select actor.languages, count(*) as count_lang from twitter group by actor.languages order by count_lang").show()
    //Get earliest and latest tweet dates
    df.select("body", "postedTime").where("postedTime IS NOT NULL").orderBy("postedTime").show(1)
    df.sqlContext.sql("select body, postedTime from twitter where postedTime IS NOT NULL order by postedTime, body DESC").show(1)
    //Get Top devices used among all Twitter users
    df.sqlContext.sql("select generator.displayName, count(*) device_count from twitter WHERE  generator.displayName IS NOT NULL group by generator.displayName order by device_count").show()
    //Find all the tweets by user
    df.sqlContext.sql("select actor.displayName, collect_list (body) from twitter WHERE  actor.displayName IS NOT NULL group by actor.displayName").show()
//    df.select("actor.displayName", "body").groupBy("displayName").sum().show()
    //Find how many tweets each user has
    df.sqlContext.sql("select actor.displayName, count(*) from twitter group by actor.displayName").show()
    //Find all the persons mentioned on tweets
    /*df.sqlContext.sql("select distinct twitter_entities.user_mentions.element.name from twitter where " +
      "twitter_entities.user_mentions.element.name is not null")*/
    //Count how many times each person is mentioned
    //
    //Find the 10 most mentioned persons
    //
    //Find all the hashtags mentioned on a tweet
    //Count how many times each hashtag is mentioned
    //Find the 10 most popular Hashtags
  }

}
