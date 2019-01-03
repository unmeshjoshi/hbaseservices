package com.dataservices

import com.hbaseservices.spark.HBaseRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkHBaseDataFrameTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {

  val hbaseTestUtility = newHbaseTestUtility

  val columnFamily: String = "cf"
  val hbaseTableName = "positions"

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }


  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();
  }

  test("should write and read hbase data with spark dataframe") {
    val sparkConf = new SparkConf().setAppName("HBaseDataframe").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val positionGenerator = new PositionsTestDataGenerator(hbaseTestUtility, columnFamily, hbaseTableName).createTable()
      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
      .seedData("10100002899999", "20-Aug-15", "MONEYMAREKTMF")

    val conf: Configuration = hbaseTestUtility.getConfiguration
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

    val hbaseRepository = new HBaseRepository(sparkSession, conf, columnFamily)
    //    writeToHBase(sc, sparkSession, conf)
    hbaseRepository.writeToHBase("10100002899999", "21-Aug-15", "MONEYMAREKTMF")

    val dataFrame = hbaseRepository.readFromHBase()
    assert(dataFrame.count() == 3)

  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
