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
    new PositionsTestDataGenerator(hbaseTestUtility)
      .createTable()
      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")

    val positions = new PositionRepository(hbaseTestUtility.getConnection).getPositionsFor("10100002899999")

    assert(positions.size == 1)
    assert(positions(0).acctKey == "10100002899999")
  }

  //TODO: Fetch product processor accountid from demographic service given acctKey?
  //Aggregateby
  // cash,
      //investment cash
         //group by currency
      //cash equivalent
         //group by cusip
      //deposits.
        //no grouping, account number/description
  // fixedincome,
     //group by cusip
  // equities,
     // group by ticker/name of equity (e.g.ABT)
  // hedge funds,
     //nogrouping
  // private equities,
     //cusip??
  // real estate,
    // no grouping
    //description(cusip, name), account, ccy, recallable distribution, isin, asset class, capital commitment, capital contributions (inside commitment), capital contribution fund expenses(outside commitment), distributions, estimated asset value +
  // other assets,
    // no grouping.
    //units, description (total, per anum due), ccy (currency), isin, cusip, next pay date, account, principal income, purchaase date, region , symbol, yield, asset class, accrued interest, average unit cost, country of issuance, current value, estimated annual income, market price,total cost basis, unrealised gain(loss)
  // liabilities,
    //no grouping..
      // description (account number, type), ccy(currency), amount you owe, interest rate, next payment amount, next due date, amount paid ytd, accrued amount, last payment date, last payment received, maturity date
  test("should aggregate total amount based on asset type") {
    new PositionsTestDataGenerator(hbaseTestUtility)
      .createTable()
      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI13")
      .seedData("10100002899999", "20-Aug-14", "MONEYMAREKTMF")
      .seedData("10100002899999", "20-Aug-14", "DCANUSNFI14")
      .seedData("10100002899999", "21-Aug-14", "DCANUSNFI14")
      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI14")

    val positions = new PositionRepository(hbaseTestUtility.getConnection).getPositionsFor("10100002899999", "assetClassCd")

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

