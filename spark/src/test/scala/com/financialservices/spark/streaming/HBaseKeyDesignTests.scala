package com.financialservices.spark.streaming

import com.dataservices.AccountPositionTestDataGenerator
import com.financialservices.AccountPositionRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually

class HBaseKeyDesignTests  extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  val hbaseTestUtility = newHbaseTestUtility

  test("should insert multiple records for given account key and instrument") {
    new AccountPositionTestDataGenerator(hbaseTestUtility).createTable()
      .seedData("1001_inst1", "1/1/2018", "100")
      .seedData("1001_inst1", "1/1/2018", "100")

    new AccountPositionRepository(hbaseTestUtility.getConnection).getPositionsFor("1001_inst1")
  }

  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();
  }

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
