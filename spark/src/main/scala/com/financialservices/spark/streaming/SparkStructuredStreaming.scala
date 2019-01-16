package com.financialservices.spark.streaming

import com.gemfire.GemfireCacheProvider
import com.financialservices.AccountPosition
import com.financialservices.spark.HbaseConnectionProperties
import com.financialservices.spark.streaming.messages.Account
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object SparkStructuredStreaming extends Serializable {

  def processStream(gemfireCacheProvider:GemfireCacheProvider, sparkSession: SparkSession, kafkaBootstrapServers: String, zookeeperConnection: HbaseConnectionProperties, kafkaTopic: String) = {

    val frame: DataFrame = kafkaMessageStream(sparkSession, kafkaBootstrapServers, kafkaTopic)

    val balance = "102.477"
    val date = "20-Aug-14"

    import sparkSession.implicits._
    val dataSet: Dataset[AccountPosition] = frame
      .map(row ⇒ {
        val xmlMessage = row.getAs[String](1)
        import com.thoughtworks.xstream.XStream
        import com.thoughtworks.xstream.io.xml.StaxDriver
        val xstream = new XStream(new StaxDriver)
        xstream.alias("account", classOf[Account])
        val accountMessage = xstream.fromXML(xmlMessage).asInstanceOf[Account]
        new AccountPosition(accountMessage.accountKey, accountMessage.amount, accountMessage.date)
      })


    val hbaseWriter = new PositionHBaseWriter(sparkSession, zookeeperConnection)
    val gemfireWriter = new GemfireWriter(sparkSession, gemfireCacheProvider)

    val hbaseStream = dataSet.writeStream.foreach(hbaseWriter).start()
    val gemfireStream = dataSet.writeStream.foreach(hbaseWriter).start()
  }

  private def kafkaMessageStream(sparkSession: SparkSession, kafkaBootstrapServers: String, kafkaTopic: String) = {

    val kafkaDataframe = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("kafka.metadata.max.age.ms", "1")
      .option("kafka.default.api.timeout.ms", "3000")
      .option("startingOffsets", "earliest") //Must for tests.
      .load()


    kafkaDataframe.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  }
}
