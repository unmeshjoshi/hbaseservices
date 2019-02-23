package com.dataservices

import com.financialservices.spark.streaming.DataPipelineTestBase
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Random

class SparkHBaseVersionedReadTest extends DataPipelineTestBase {

  private def uniqueRowKey(acctKey: String, date: String) = {
    s"${acctKey}_${date}_${new Random().nextInt()}"
  }

  test("should write data marked by specific timestamp version and read latest data for given key") {
    val sparkConf = new SparkConf().setAppName("HBaseDataframe").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
      .seedData("10100002899999", "19-Aug-14", "100", 100, "rowKey1")
      .seedData("10100002899999", "19-Aug-14", "110", 200, "rowKey1")

    val conf: Configuration = hbaseTestUtility.getConfiguration
    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))
    val dataFrame = hbaseRepository.readFromHBase("rowKey1")

    val rows = dataFrame.collectAsList()
    assert(rows.size() == 1)
    val balance = rows.get(0).get(0)
    assert(balance == "110")
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }
}
