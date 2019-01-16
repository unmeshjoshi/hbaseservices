package com.financialservices.spark.streaming

import java.util.{Properties, UUID}

import com.financialservices.util.Networks
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.duration._

abstract class DataPipelineTestBase extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  implicit val patience = PatienceConfig(20.seconds, 1.seconds)
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig
  val hbaseTestUtility = newHbaseTestUtility

  override def afterAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.stop()
    hbaseTestUtility.shutdownMiniCluster()
  }

  def createKafkaConfig: EmbeddedKafkaConfig = {
    val kafkaHost = new Networks().hostname()
    val kafkaPort = 9002

    def defaultBrokerProperties(hostName: String) = {
      val brokers = s"PLAINTEXT://$hostName:$kafkaPort"
      Map("listeners" → brokers, "advertised.listeners" → brokers,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG → "1",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG → "false",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG → "earliest")
    }

    EmbeddedKafkaConfig(customBrokerProperties = defaultBrokerProperties(kafkaHost))
  }

  def createProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("application.id", UUID.randomUUID().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def bootstrapServers = {
    val kafkaHost = new Networks().hostname()
    val kafkaPort = 9002

    s"${kafkaHost}:${kafkaPort}"
  }

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    val cluster = hbaseTestUtility.startMiniCluster()
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
