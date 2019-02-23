package com.dataservices

import com.financialservices.spark.streaming.DataPipelineTestBase
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession




class SparkHBaseDataFrameTest extends DataPipelineTestBase {

  test("should write and read hbase data with spark dataframe") {
    val sparkConf = new SparkConf().setAppName("HBaseDataframe").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility).createTable()
      .seedData("10100002899999", "19-Aug-14", "100")
      .seedData("10100002899999", "20-Aug-15", "110")

    val conf: Configuration = hbaseTestUtility.getConfiguration
    val hbaseRepository = new HBaseRepository(sparkSession, HbaseConnectionProperties(conf))
    hbaseRepository.writeToHBase("10100002899999", "21-Aug-15", "120")

    val dataFrame = hbaseRepository.readFromHBase(Map(("balance", "120")))
    assert(dataFrame.count() == 1)

  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
