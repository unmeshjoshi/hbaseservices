import com.dataservices.AccountPositionTestDataGenerator
import com.hbaseservices.spark.streaming.{DataPipelineTestBase, SparkStructuredStreaming}
import com.hbaseservices.spark.{HBaseRepository, HbaseConnectionProperties}
import com.test.gemfire.TestGemfireCache
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SparkStreamTest extends DataPipelineTestBase {
  val positionsTestDataGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility)
  override protected def beforeAll() = {
    super.beforeAll()
    positionsTestDataGenerator.createTable()
  }

  test("should consume messages from spark") {

    positionsTestDataGenerator.seedData("10100002899999", "19-Aug-14", "100")

    produceTestMessagesSync("memberTopic")

    val conf: Configuration = hbaseTestUtility.getConfiguration

    val appName = "StructuredKafkaProcessing"
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()

    val gemfireCache = new TestGemfireCache()

    SparkStructuredStreaming.processStream(gemfireCache, sparkSession, bootstrapServers, HbaseConnectionProperties(conf), "memberTopic")

    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))

    eventually {
      val dataFrame = hbaseRepository.readFromHBase("10100002899999")
      assert(dataFrame.count() == 1)
    }

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