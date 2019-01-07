package com.hbaseservices.spark.streaming

import com.hbaseservices.Position
import com.hbaseservices.spark.HBaseRepository
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.hadoop.hbase.client.Connection


object SparkStructuredStreaming extends Serializable {

  def processStream(sparkSession:SparkSession, kafkaBootstrapServers: String, kafkaTopic: String, conf:Configuration, hbaseTableName:String) = {

    import sparkSession.implicits._

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("startingOffsets", "earliest") //Must for tests.
      .load()
    val relationshipKey = "10500000746112"
    val egKey = "10300000692192"
    val alKey = "1000566819412"
    val nomCcyCd = "USD"
    val refCcyCd = "USD"
    val marketUnitPriceAmount = "102.477"
    val marketPriceDate = "19-Aug-14"

    val frame: DataFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val dataSet: Dataset[Position] = frame
      .map((row) ⇒ new Position("10100002899999", egKey, marketUnitPriceAmount, marketPriceDate))

    val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"

    val writer = new HBaseWriter(sparkSession, conf, conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM), conf.getInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 0), hbaseTableName)

    val query: StreamingQuery = dataSet.writeStream.foreach(writer).start()
  }
}
