package edu.uta

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

case class Record(comment: String, rating: Double)

case class TFIDF(vector: List[Float], rating: Double)

case class IDF(term: String, doesExist: Int)

object Main {
  val stopwords = List("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
    "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them",
    "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is",
    "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a",
    "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
    "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up",
    "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when",
    "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
    "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BGReview ETL")
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
      .csv(args.head)

    val stopw = sc.broadcast(stopwords)
    val numTopWords = sc.broadcast(args(1).toInt)
    val samplingFrac = sc.broadcast(args(2).toFloat)

    val cleanedData = data.filter($"comment" isNotNull)
    val trainData = cleanedData.sample(withReplacement = false, fraction = samplingFrac.value)
      .map(row => {
        Record(
          comment = row.getAs[String]("comment")
            .toLowerCase()
            .replaceAll("</?\\w+/?>", "")
            .replaceAll("https?://\\w+.\\w+.\\w+", "")
            .replaceAll("('|\\d)", "")
            .replaceAll("\\W+", " ")
            .split("\\s+")
            .filter(_.length >= 3)
            .filterNot(stopw.value.contains).mkString(" "),
          rating = row.getAs[Double]("rating")
        )
      })
    trainData.coalesce(1).write.json("src/main/resources/_processed/JSON/data")

    /*val topWords = trainData.flatMap(row => row.comment.toList)
      .groupBy("value").count().orderBy(desc("count"))
      .limit(numTopWords.value).toDF("topword", "topwordcount")

    val termIDF = trainData.flatMap(_.comment.toSet)
      .groupBy("value")
      .count().toDF("word", "count")
      .join(topWords, $"word" === $"topword", "right")
      .select($"word" as "term", log(lit(2638172) / $"count") as "IDF")

    val trainTFIDF = trainData.crossJoin(termIDF.agg(collect_list(concat_ws("$", $"term", $"IDF"))))
      .map(row => {
        val terms = row.getList[String](2)
        val comments = row.getList[String](0)
        TFIDF(
          rating = row.getDouble(1),
          vector = terms.map(tupl => {
            val split = tupl.split("\\$")
            val (term, termIDF) = (split(0), split(1).toDouble)
            val tf = comments.count(_ == term).toDouble / comments.length
            (tf * termIDF).toFloat
          }).toList
        )
      })
    termIDF.coalesce(1).write.json("src/main/resources/_processed/JSON/IDF")
    trainTFIDF.coalesce(1).write.json("src/main/resources/_processed/JSON/TrainData")

    spark.stop()
    sc.stop()*/
  }
}
