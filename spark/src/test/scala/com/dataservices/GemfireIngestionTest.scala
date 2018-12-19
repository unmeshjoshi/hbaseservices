package com.dataservices

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers}

class GemfireIngestionTest extends QueryTest with SharedSparkSession with BeforeAndAfterAll with Matchers with Eventually {
  val hbaseTestUtility = newHbaseTestUtility

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }

  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }
//
//  test("Should ingest all cash positions to gemfire") {
//    new PositionsTestDataGenerator(hbaseTestUtility)
//      .createTable()
//      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
//      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI13")
//      .seedData("10100002899999", "20-Aug-14", "MONEYMAREKTMF")
//      .seedData("10100002899999", "20-Aug-14", "DCANUSNFI14")
//      .seedData("10100002899999", "21-Aug-14", "DCANUSNFI14")
//      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI14")
//  }

}
