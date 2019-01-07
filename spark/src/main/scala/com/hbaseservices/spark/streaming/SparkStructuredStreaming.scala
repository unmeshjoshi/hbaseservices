package com.hbaseservices.spark.streaming

import com.gemfire.GemfireCacheProvider
import com.hbaseservices.Position
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object SparkStructuredStreaming extends Serializable {

  def processStream(gemfireCacheProvider:GemfireCacheProvider, sparkSession: SparkSession, kafkaBootstrapServers: String, kafkaTopic: String, conf:Configuration, hbaseTableName:String) = {

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

    val hbaseWriter = new HBaseWriter(sparkSession, conf, conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM), conf.getInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 0), hbaseTableName)
    val gemfireWriter = new GemfireWriter(sparkSession, gemfireCacheProvider)

    val hbaseStream = dataSet.writeStream.foreach(hbaseWriter).start()
    val gemfireStream = dataSet.writeStream.foreach(hbaseWriter).start()
  }
}
