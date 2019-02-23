package com.financialservices.spark.streaming

import com.dataservices.AccountPositionTestDataGenerator
import com.financialservices.AccountPositionRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually

class HBaseKeyDesignTests  extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {
  private val hbaseTestUtility = newHbaseTestUtility


  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();

  }

  before {
    new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
  }

  after {
    new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).deleteTable()
  }

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }

  test("should insert multiple records for same account, instrument for different date") {
    val accountNumber = "1001"
    val instrument = "inst1"
    val accountKey = s"${accountNumber}_${instrument}"
    new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection)
      .seedData(accountKey, "1/1/2018", "100")
      .seedData(accountKey, "2/1/2018", "100")

    val positions = new AccountPositionRepository(hbaseTestUtility.getConnection).getAllPositionsFor(accountKey)
    assert(2 == positions.size)
  }

  test("should be able to get latest valid record for given, account, instrument and transaction date") {
    val accountNumber = "1001"
    val instrument = "inst1"
    val accountKey = s"${accountNumber}_${instrument}"

    new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection)
      .seedData(accountKey, "1/1/2018", "100")
      .seedData(accountKey, "2/1/2018", "102")

    val positions = new AccountPositionRepository(hbaseTestUtility.getConnection).getPositionsFor(accountKey, "2/1/2018")
    assert(1 == positions.size)
    assert("102" == positions(0).balance)
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
