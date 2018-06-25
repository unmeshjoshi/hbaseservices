package com.dataservices

import com.hbaseservices.PositionRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class DataServiceTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  val hbaseTestUtility = newHbaseTestUtility

  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();
  }

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }

  test("should get list of positions for account key") {
    new PositionsTestDataGenerator(hbaseTestUtility).seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")

    val positions = new PositionRepository(hbaseTestUtility.getConnection).getPositionsFor("10100002899999")

    assert(positions.size == 1)
    assert(positions(0).acctKey == "10100002899999")
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }
}

