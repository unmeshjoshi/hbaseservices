import com.dataservices.AccountPositionTestDataGenerator
import com.financialservices.spark.streaming.{DataPipelineTestBase, SparkStructuredStreaming}
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import com.test.gemfire.TestGemfireCache
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SparkStructuredStreamingTest extends DataPipelineTestBase {
  var positionsTestDataGenerator:AccountPositionTestDataGenerator = _

  override protected def beforeAll() = {
    super.beforeAll()
    positionsTestDataGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection)
    positionsTestDataGenerator.createTable()
  }

  test("should consume messages from spark") {


    positionsTestDataGenerator.seedData("10100002899999", "19-Aug-14", "100")
    val kafkaTopic = "financials"
    produceTestMessagesSync(topic = kafkaTopic, accountNumber = "10100002899999", noOfMessages = 10)

    val conf: Configuration = hbaseTestUtility.getConfiguration

    val appName = "StructuredKafkaProcessing"
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local")

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()

    val gemfireCache = new TestGemfireCache()

    SparkStructuredStreaming.processStream(gemfireCache, sparkSession, bootstrapServers, HbaseConnectionProperties(conf), kafkaTopic, shouldFail = true)

    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))

    eventually {
      val dataFrame = hbaseRepository.readFromHBase("10100002899999")
      assert(dataFrame.count() == 50)
    }

  }

  private def produceTestMessagesSync(topic: String, accountNumber: String, noOfMessages: Int): Unit = {

    val producer = createProducer

    for (i ← 1 to noOfMessages) {
      val position = s"<account>" +
        s"<num>${i}</num>" +
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