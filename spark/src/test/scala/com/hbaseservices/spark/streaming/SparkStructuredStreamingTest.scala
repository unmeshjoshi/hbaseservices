import com.dataservices.PositionsTestDataGenerator
import com.hbaseservices.spark.HBaseRepository
import com.hbaseservices.spark.streaming.{DataPipelineTestBase, SparkStructuredStreaming}
import com.test.gemfire.TestGemfireCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SparkStreamTest extends DataPipelineTestBase {
  val columnFamily: String = "cf"
  val hbaseTableName = "positions"

  override protected def beforeAll() = {
    super.beforeAll()
    new PositionsTestDataGenerator(hbaseTestUtility, columnFamily, hbaseTableName).createTable()
  }

  test("should consume messages from spark") {

    produceTestMessagesSync("memberTopic")

    val conf: Configuration = hbaseConfiguration()

    val appName = "StructuredKafkaProcessing"
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()


    val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"
    val hbaseZookeeperQuorum = conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM)
    val hbaseZookeerClientPort = conf.getInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 0)

    val gemfireCache = new TestGemfireCache()
    SparkStructuredStreaming.processStream(gemfireCache, sparkSession, bootstrapServers, hbaseZookeeperQuorum, hbaseZookeerClientPort, "memberTopic", hbaseTableName)

    val hbaseRepository = new HBaseRepository(sparkSession, columnFamily, hbaseZookeeperQuorum, hbaseZookeerClientPort, hbaseTableName)

    eventually {
      val dataFrame = hbaseRepository.readFromHBase(Map(("alKey", "1000566819412")))
      assert(dataFrame.count() == 1)
    }

  }

  private def hbaseConfiguration ()= {
    val conf: Configuration = hbaseTestUtility.getConfiguration
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf
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
}