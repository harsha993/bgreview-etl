package edu.uta

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class Record(comment: List[String], game: String, rating: Double)

object Main {
  val stopwords = List("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
    "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
    "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be",
    "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because",
    "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
    "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some",
    "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should"
    , "now")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BGReview ETL")
      .set("spark.driver.memory", "6g")
      .set("spark.default.parallelism", "6")
      .set("spark.driver.cores", "6")
      .setMaster("local[6]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("BGReview ETL")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //    Read the dataset into a Spark-Dataframe
    val data = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/bgg-13m-reviews.csv")

    val stopw = sc.broadcast(stopwords)

    val cleanedData = data.filter($"comment" isNotNull).map(row => {
      Record(
        game = row.getAs[String]("name"),
        comment = row.getAs[String]("comment")
          .toLowerCase()
          .replaceAll("</?\\w+/?>", "")
          .replaceAll("https?://\\w+.\\w+.\\w+", "")
          .replaceAll("\\W+", " ")
          .split("\\s+").filterNot(stopw.value.contains).toList,
        rating = row.getAs[Double]("rating")
      )
    })

    val wordCounts = cleanedData.flatMap(row => row.comment.toList)
      .groupBy("value").count().orderBy(desc("count"))
    val topWords = wordCounts.limit(2000).agg(collect_list("value").as("topWords"))
    val rareWords = wordCounts.filter($"count" <= 5).agg(collect_list("value").as("rarewords"))

    /*    val withoutRareWords = cleanedData.crossJoin(rareWords).map(row=>{
          val rare = row.getList[String](3)
          val cmt = row.getList[String](0).asScala.filterNot(rare.contains).toList
          Record(
            game= row.getAs[String]("game"),
            comment = cmt,
            rating = row.getAs[Double]("rating")
          )
        })*/
    wordCounts.write.json("src/main/resources/_processed/JSON/wordcounts")
    rareWords.write.json("src/main/resources/_processed/JSON/rarewords_10")
    cleanedData.write.json("src/main/resources/_processed/JSON/cleandata")

  }
}
