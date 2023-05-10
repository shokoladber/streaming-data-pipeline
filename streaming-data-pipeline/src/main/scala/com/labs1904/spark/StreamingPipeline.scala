package com.labs1904.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

case class RawReview(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String,
                     product_title: String, product_category: String, star_rating: String, helpful_votes: String,
                     total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String,
                     review_date: String)

/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  val hdfsUrl = "hdfs://hbase01.hourswith.expert:8020/"
  val bootstrapServers = "b-3-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-2-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196,b-1-public.hwekafkacluster.6d7yau.c16.kafka.us-east-1.amazonaws.com:9196"
  val username = "1904labs"
  val password = "1904labs"
  val hdfsUsername = "mallen" // TODO: set this to your handle

  //Use this for Windows
//  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
        .appName(jobName)
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val ds = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      // TODO: implement logic here
      val results = ds
      val rawReviews = results.map(result=>{
        val splitReview = result.split("\\t")
        RawReview(splitReview(0), splitReview(1), splitReview(2), splitReview(3), splitReview(4), splitReview(5),  splitReview(6), splitReview(7), splitReview(8), splitReview(9), splitReview(10), splitReview(11), splitReview(12), splitReview(13), splitReview(14))
      })

      val enrichedReviews = rawReviews.mapPartitions(partition=>{
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "hbase01.labs1904.com:2181")
        val connection = ConnectionFactory.createConnection(conf)
      })

      // Write output to console
      val query = reviews.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      // Write output to HDFS
//      val query = result.writeStream
//        .outputMode(OutputMode.Append())
//        .format("json")
//        .option("path", s"/user/${hdfsUsername}/reviews_json")
//        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()
      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def getScramAuthString(username: String, password: String) = {
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    val arrOfWords = sentence.split("\\W+")
    val wordsToLowerCase = arrOfWords.map(word => word.toLowerCase)
    wordsToLowerCase
  }
}
