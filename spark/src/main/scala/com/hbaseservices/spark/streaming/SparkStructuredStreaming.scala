package com.hbaseservices.spark.streaming

import com.gemfire.GemfireCacheProvider
import com.hbaseservices.Position
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkStructuredStreaming extends Serializable {

  def processStream(gemfireCacheProvider:GemfireCacheProvider, sparkSession: SparkSession, kafkaBootstrapServers: String, zookeeperQuorum: String, hbaseZookeeperClientPort: Int, kafkaTopic: String, hbaseTableName:String) = {

    import sparkSession.implicits._

    val kafkaDataframe = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("startingOffsets", "earliest") //Must for tests.
      .load()


    val frame: DataFrame = kafkaDataframe.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val egKey = "10300000692192"
    val marketUnitPriceAmount = "102.477"
    val marketPriceDate = "19-Aug-14"

    val dataSet: Dataset[Position] = frame
      .map((row) ⇒ new Position("10100002899999", egKey, marketUnitPriceAmount, marketPriceDate))


    val hbaseWriter = new HBaseWriter(sparkSession, zookeeperQuorum, hbaseZookeeperClientPort, hbaseTableName)
    val gemfireWriter = new GemfireWriter(sparkSession, gemfireCacheProvider)

    val hbaseStream = dataSet.writeStream.foreach(hbaseWriter).start()
    val gemfireStream = dataSet.writeStream.foreach(hbaseWriter).start()
  }
}
