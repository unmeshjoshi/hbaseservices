package com.dataservices

import com.financialservices.AccountPosition
import com.financialservices.spark.streaming.DataPipelineTestBase
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer




class SparkHBaseDataFrameTest extends DataPipelineTestBase {

  test("should be able to read from hbase based on dataframe created from row keys") {
    val sparkConf = new SparkConf().setAppName("spark-dataframe-tests").setMaster("local[*]")

    val session = SparkSession
      .builder
      .config(sparkConf).getOrCreate()

    //this simulates hbase load and storing keys file on hdfs
    val accountKeysAndRowKeys = loadDataToHbaseAndGetRowKeys()


    val conf: Configuration = hbaseTestUtility.getConfiguration
    val hbaseRepository = new HBaseRepository(session, HbaseConnectionProperties(conf))

    //create dataframe for row keys ordered ascending so that we can use startRow, endRow on hbase query.
    val rowKeysDataFrame = createRowKeysDataframeOrderedByRowKeyAscending(session, accountKeysAndRowKeys._2)

    //for each partition on input keys, load data from hbase for given key range.
    val dataSet: Dataset[AccountPosition] = hbaseRepository.readFromHBaseFor(rowKeysDataFrame)

    import scala.collection.JavaConverters._

    val positions = dataSet.collectAsList().asScala

    assert(100 == positions.size)

    val expectedAccountKeys = accountKeysAndRowKeys._1
    expectedAccountKeys.foreach(expectedAccountKey ⇒ {
      val accountKeys = positions.map(p ⇒ p.acctKey)

      assert(accountKeys.contains(expectedAccountKey))
    })
   }

  test("should write and read hbase data with spark dataframe") {
    val sparkConf = new SparkConf().setAppName("HBaseDataframe").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
    (1 to 100).foreach(i ⇒ {
      val accountNumberBase = "101000028999"
      val accountKey = s"${accountNumberBase}${i}"
      positionGenerator
        .seedData(accountKey, "19-Aug-14", "100")
    })

//      .seedData("10100002899999", "19-Aug-14", "100")
//      .seedData("10100002899999", "20-Aug-15", "110")

    val conf: Configuration = hbaseTestUtility.getConfiguration
    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))
    hbaseRepository.writeToHBase("101000028999", "21-Aug-15", "120")


    val dataFrame = hbaseRepository.readFromHBase(Map(("balance", "120")))
    assert(dataFrame.count() == 1)

  }


  private def uniqueRowKey(acctKey: String, date: String) = {
    import java.security.MessageDigest
    val hash = "35454B055CC325EA1AF2126E27707052"
    val md = MessageDigest.getInstance("MD5")
    md.update(acctKey.getBytes)
    import javax.xml.bind.DatatypeConverter
    val digest = md.digest
    val myHash = DatatypeConverter.printHexBinary(digest).toUpperCase

    s"${myHash}_${acctKey}_${date}"
  }
  private def createRowKeysDataframeOrderedByRowKeyAscending(session: SparkSession, rowKeys: ListBuffer[String]) = {
    val schema = List(StructField("rowKey", StringType, true))
    val dataframeBuilder = new DataFrameBuilderS(schema)
    //Need TO order by to make sure its lexicographically ordered so that we can use startRow and endRow filter in hbase.
    val criteriaDf = dataframeBuilder.createDataFrame(session, rowKeys.toSeq).orderBy("rowKey")
    criteriaDf
  }

  private def loadDataToHbaseAndGetRowKeys() = {
    val accountKeys = new ListBuffer[String]
    //this simulates rowKeys file on hdfs, as output from the hbase load.
    val rowKeys = new ListBuffer[String]()

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
    (1 to 100).foreach(i ⇒ {
      val accountNumberBase = "101000028999"
      val accountKey = s"${accountNumberBase}${i}"
      val date = "19-Aug-14"
      val rowKey: String = uniqueRowKey(accountKey, date)
      positionGenerator
        .putRow(accountKey, date, "100", "0", Long.MaxValue, rowKey)

      rowKeys += rowKey
      accountKeys += accountKey
    })
    (accountKeys, rowKeys)
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
