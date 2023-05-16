package com.labs1904.spark

import com.labs1904.spark
import com.labs1904.spark.StreamingPipeline.username
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
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
                          review_date: String, birthdate: String, email: String, name: String, sex: String, customerUsername: String)

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
      //start spark session
      val spark = SparkSession.builder()
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
        val splitReview = result.split("\\t")
        RawReview(splitReview(0), splitReview(1), splitReview(2), splitReview(3), splitReview(4), splitReview(5),  splitReview(6), splitReview(7), splitReview(8), splitReview(9), splitReview(10), splitReview(11), splitReview(12), splitReview(13), splitReview(14))
      })

      //connect spark to hbase; get table "mallen:users"
      //map rawReview to mallen:users with spark map by partitions
      val enrichedReviews = rawReviews.mapPartitions(partition=>{
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "hbase01.labs1904.com:2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("mallen:users"))

        //map rawReview lines to mallen:users by review_id
        //enrich rawReview, combining review_id w/ customer_id info
        val iter = partition.map(review=>{
          val getReviews = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
          val resultReviews = table.get(getReviews)

          //create customer info variables
          val birthdate = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate")))
          val email = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
          val name = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name")))
          val sex = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex")))
          val customerUsername = Bytes.toString(resultReviews.getValue(Bytes.toBytes("f1"), Bytes.toBytes("customerUsername")))

          //add hbase customer columns to hbase table w/ review data
          val put = new Put(Bytes.toBytes(review.customer_id))
            .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("birthdate"), Bytes.toBytes(birthdate))
            .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("email"), Bytes.toBytes(email))
            .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(name))
            .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes(sex))
            .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("customer_username"), Bytes.toBytes(customerUsername))
          table.put(put)

          val scan = new Scan()
          val scanner = table.getScanner(scan)
          val enrichedReviews = scanner.forEach(scannedReview => {
            EnrichedReview(Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("marketplace"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("customer_id"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("review_id"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_id"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_parent"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_title"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_category"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("star_rating"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("helpful_votes"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("total_votes"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("vine"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("verified_purchase"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("review_headline"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("review_body"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("review_date"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("email"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex"))),
              Bytes.toString(scannedReview.getValue(Bytes.toBytes("f1"), Bytes.toBytes("customer_username")))
            )
          })
          enrichedReviews
        }).toList.iterator

        connection.close()
        iter
      })

      // Write output to console
      val query = enrichedReviews.writeStream
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
