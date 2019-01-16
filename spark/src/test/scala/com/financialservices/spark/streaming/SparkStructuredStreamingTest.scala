import com.dataservices.AccountPositionTestDataGenerator
import com.financialservices.spark.streaming.{DataPipelineTestBase, SparkStructuredStreaming}
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import com.test.gemfire.TestGemfireCache
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SparkStructuredStreamingTest extends DataPipelineTestBase {
  val positionsTestDataGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility)
  override protected def beforeAll() = {
    super.beforeAll()
    positionsTestDataGenerator.createTable()
  }

  test("should consume messages from spark") {

    positionsTestDataGenerator.seedData("10100002899999", "19-Aug-14", "100")

    val kafkaTopic = "financials"
    produceTestMessagesSync(topic=kafkaTopic, accountNumber = "10100002899999", notOfMessages = 10)

    val conf: Configuration = hbaseTestUtility.getConfiguration

    val appName = "StructuredKafkaProcessing"
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()

    val gemfireCache = new TestGemfireCache()

    SparkStructuredStreaming.processStream(gemfireCache, sparkSession, bootstrapServers, HbaseConnectionProperties(conf), kafkaTopic)

    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))

    eventually {
      val dataFrame = hbaseRepository.readFromHBase("10100002899999")
      assert(dataFrame.count() == 11)
    }

  }

  private def produceTestMessagesSync(topic: String, accountNumber: String, notOfMessages:Int): Unit = {

    val producer = createProducer

    for (i ← 1 to notOfMessages) {
      val position = s"<account>" +
        s"<accountKey>${accountNumber}</accountKey>" +
        s"<amount>${(i * 100) + 1000}</amount>" +
        "<accountType>SAVINGS</accountType>" +
        s"<date>${i}-1-2018</date>" +
        s"<time>${i}:00</time>" +
        "</account>"

      val data = new ProducerRecord[String, String](topic, s"key${i % 2}", position)
      val value = producer.send(data)
      println(value.get().serializedValueSize()) //blocking send
    }
    producer.close()
  }
}