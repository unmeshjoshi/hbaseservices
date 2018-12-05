package com.dataservices

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class HBaseTimeStampTestextends extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  val hbaseTestUtility = newHbaseTestUtility
//
//  test("should give timestamp for new row") {
//    new PositionsTestDataGenerator(hbaseTestUtility)
//      .createTable()
//      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
//  }

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

}
