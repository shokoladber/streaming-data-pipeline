package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

case class RawReview(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String,
                     product_title: String, product_category: String, star_rating: String, helpful_votes: String,
                     total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String,
                     review_date: String)

case class EnrichedReview(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String,
                          product_title: String, product_category: String, star_rating: String, helpful_votes: String,
                          total_votes: String, vine: String, verified_purchase: String, review_headline: String, review_body: String,
                          review_date: String, birthdate: String, email: String, name: String, sex: String, customer_username: String)

/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"
  val hdfsUrl = "hdfs://hbase01.labs1904.com:2181/"
  val bootstrapServers = "change me"
  val username = "1904labs"
  val password = "1904labs"
  val hdfsUsername = "mallen" // TODO: set this to your handle

  //Use this for Windows
//  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  def main(args: Array[String]): Unit = {
    try {
      //start spark session
      val spark = SparkSession.builder()
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.fs.defaultFS", hdfsUrl)
        .config("spark.sql.shuffle.partitions", "3")
        .appName(jobName)
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      //setup kafka stream
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
      // map kafka data into RawReview case class
      val results = ds
      val rawReviews = results.map(result=>{
        val splitReview = result.split("\t")
        RawReview(splitReview(0), splitReview(1), splitReview(2), splitReview(3), splitReview(4), splitReview(5),  splitReview(6), splitReview(7), splitReview(8), splitReview(9), splitReview(10), splitReview(11), splitReview(12), splitReview(13), splitReview(14))
      })

      //connect spark to hbase; get table "mallen:users"
      //map rawReview to mallen:users with spark map by partitions
      val enrichedReviewsDS: Dataset[EnrichedReview] = rawReviews.mapPartitions(partition=>{
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "hbase01.labs1904.com:2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("mallen:users"))

        //map rawReview lines to mallen:users by customer_id
        //enrich rawReview, combining review from kafka w/ customer info from hbase
        val enrichedReviewsIter: Iterator[EnrichedReview] = partition.map(review => {
          val getReviews = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
          val resultReviews = table.get(getReviews)

          //create customer info variables
          val birthdate = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate")))
          val email = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
          val name = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name")))
          val sex = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex")))
          val customerUsername = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("username")))

          EnrichedReview(review.marketplace, review.customer_id, review.review_id, review.product_id, review.product_parent, review.product_title, review.product_category, review.star_rating, review.helpful_votes, review.total_votes, review.vine, review.verified_purchase, review.review_headline, review.review_body, review.review_date, birthdate, email, name, sex, customerUsername)
        })

        val enrichedReviewsList = enrichedReviewsIter.toList

        connection.close()

        enrichedReviewsList.iterator
      })

      // Write output to console
//      val query = enrichedReviewsDS.writeStream
//        .outputMode(OutputMode.Append())
//        .format("console")
//        .option("truncate", false)
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()

      // Write output to HDFS
      val query = enrichedReviewsDS.writeStream
        .outputMode(OutputMode.Append())
        .format("csv")
        .option("delimiter", ",")
        .option("path", s"/user/${hdfsUsername}/reviews_csv")
        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
        .partitionBy("star_rating")
        .trigger(Trigger.ProcessingTime("20 seconds"))
        .start()
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
