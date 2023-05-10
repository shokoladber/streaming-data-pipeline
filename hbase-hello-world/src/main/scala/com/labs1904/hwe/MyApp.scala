package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbase01.labs1904.com:2181")
      connection = ConnectionFactory.createConnection(conf)

      // Example code... change me

      // setup table connection
      val table = connection.getTable(TableName.valueOf("mallen:users"))

      //get birthdate from row 10000001
      val get = new Get(Bytes.toBytes("10000001"))
      val result = table.get(get)
      val birthdate = {
        Bytes.toString(
          result.getValue(Bytes.toBytes("f1": String), Bytes.toBytes("birthdate": String))
        )
      }

      // create new user
      val put99 = new Put(Bytes.toBytes("99"))
        .addColumn(Bytes.toBytes("f1": String), Bytes.toBytes("favorite_color": String), Bytes.toBytes("pink"))
        .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("username"), Bytes.toBytes("DE-HWE"))
        .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("The Panther"))
        .addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("F"))

      //add new user to table
      table.put(put99)

      //get new user info
      val get99 = new Get(Bytes.toBytes("99"))
      val result99 = table.get(get99)

      //create scanner
      val scan = new Scan()
        .withStartRow(Bytes.toBytes("10000001"))
        .withStopRow(Bytes.toBytes("10006001"))
        .getBatch
//      val scanner = table.getScanner(scan).forEach(_=> Bytes.toString(_))


      // log results
      logger.debug(result)
      logger.debug(birthdate)
      logger.debug(result99)
      logger.debug(scan)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}
