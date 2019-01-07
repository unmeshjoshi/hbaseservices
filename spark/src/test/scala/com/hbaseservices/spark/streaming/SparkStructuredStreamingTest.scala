import java.util.{Properties, UUID}

import com.dataservices.PositionsTestDataGenerator
import com.gemfire.PositionCache
import com.hbaseservices.spark.HBaseRepository
import com.hbaseservices.spark.streaming.SparkStructuredStreaming
import com.hbaseservices.util.Networks
import com.test.gemfire.{TestGemfireCache, TestGemfireCacheProvider}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class SparkStreamTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  implicit val patience = PatienceConfig(10.seconds, 1.seconds)
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = createKafkaConfig


  val hbaseTestUtility = newHbaseTestUtility

  val columnFamily: String = "cf"
  val hbaseTableName = "positions"

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    hbaseTestUtility.startMiniCluster();
    new PositionsTestDataGenerator(hbaseTestUtility, columnFamily, hbaseTableName).createTable()
  }

  override def afterAll(): Unit = {
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

  def bootstrapServers = {
    val kafkaHost = new Networks().hostname()
    val kafkaPort = 9002

    s"${kafkaHost}:${kafkaPort}"
  }

  test("should consume messages from spark") {

    produceTestMessagesSync("memberTopic")

    val conf: Configuration = hbaseTestUtility.getConfiguration

    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

    val appName = "StructuredKafkaProcessing"
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()

    SparkStructuredStreaming.processStream(new TestGemfireCache(), sparkSession, bootstrapServers, "memberTopic", conf, hbaseTableName)

    val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"


    val hbaseRepository = new HBaseRepository(sparkSession, conf, columnFamily, conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM), conf.getInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 0), hbaseTableName)



    Thread.sleep(5000)

    val dataFrame = hbaseRepository.readFromHBase(Map(("alKey", "1000566819412")))
    assert(dataFrame.count() == 1)
  }

  private def produceTestMessagesSync(topic: String) = {

    val producer = createProducer

    val position = "<position>" +
      "<accountKey>10102022020</accountKey>" +
      "<nomAmount>120000000</nomAmount>" +
      "<accountType>SAVINGS</accountType>" +
      "</position>"

    val transaction = "<transaction>" +
      "<accountKey>10102022020</accountKey>" +
      "<txnAmount>10000</txnAmount>" +
      "<txnType>CREDIT</txnType>" +
      "<accountType>SAVINGS</accountType>" +
      "</transaction>"

    for (i ← 0 to 10) {
      val data = new ProducerRecord[String, String](topic, s"key${i % 2}", s"value ${i}")
      val value = producer.send(data)
      println(value.get().serializedValueSize()) //blocking send
    }
    producer.close()
  }

  private def createProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("application.id", UUID.randomUUID().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    producer
  }



  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }
}