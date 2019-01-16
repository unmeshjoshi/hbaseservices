package com.dataservices

import com.financialservices.AccountPositionRepository
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



//
  test("should get list of positions for account key") {
    new AccountPositionTestDataGenerator(hbaseTestUtility, "cf", "Positions")
          .createTable()
          .seedData("10100002899999", "19-Aug-14", "100")

    val c = hbaseTestUtility.getConnection

    val positions = new AccountPositionRepository(c).getPositionsFor("10100002899999", "19-Aug-14")

    assert(positions.size == 1)
    assert(positions(0).acctKey == "10100002899999")
  }


  test("should get specific version of position") {
    val generator = new AccountPositionTestDataGenerator(hbaseTestUtility, "cf", "Positions")
      .createTable()
    val t1 = generator
          .seedData("10100002899999", "19-Aug-14", "100")
    generator
          .seedData("10100002899999", "19-Aug-14", "100")

    val c = hbaseTestUtility.getConnection

    val positions = new AccountPositionRepository(c).getPositionsFor("10100002899999", "19-Aug-14")

    assert(positions.size == 2)
    assert(positions(0).acctKey == "10100002899999")
  }
//
//  //TODO: Fetch product processor accountid from demographic service given acctKey?
//  //Aggregateby
//  // cash,
//  //investment cash
//  //group by currency
//  //ccy, description, account,current value, accrued interest, market value, exchange rate, rate,   <currency value is shown below this value>
//  //USD, USD, C7B017004001, 27,750,039.90, 0.0, 27,750,039.90, 1.0,
//  //USD, USD, 15C065882768, 27,750,039.90, 0.0, 27,750,039.90, 1.0,
//  //cash equivalent
//  //group by cusip
//  // description, ccy, account, quantity/principal, total cost basis, current value, maturity date, rate, accrued interest, asset class, average or unit cost, cusip, estimated annual income, interest at maturity, isin, market price, principal income, purchase date, symbol, unrelaised gain(loss), yield%
//  //WESTERN ASSET INST GOV'T RESERVES
//  //CUSIP 52470G791 - cash sweep, USD, 15C065882768(trust account), 741,846.780, 490,444.96, 490,839.77, "", 1.14%,
//  //deposits.
//  //no grouping, account number/description
//  //description, ccy, account balance, accrued interest, annual percent yield, interest received ytd, account balance previous month end, account open date, asset class, available balance, interest frequency, interest rate, maturity amount, maturity date, overdraft credit limit, term, tenor, uncollected funds
//  // fixedincome,
//  //group by cusip
//  //units, description, ccy, account, total cost basis, market price, current value, unrelaised gain/loss, accrued interest, asset class, country of issuance, average or unit cost, current yield %, cusip, estimated annumal income,  nterest divident at next pay date, isin, market value, maturity date, next pay date, principa/income, purchase date, ratem region, symbol, ytm%
//  // equities,
//  // group by ticker/name of equity (e.g.ABT)
//  //units, description, ccy, account, total cost basis, market price, current value, unrealised gain(loss), yield, accrued interest, average or unit cost, assetclass, country of issuance, cusip, estimated annual income, interest divident at next pay date, isin, maturity date, next pay date, principal income, purchase date, region sector, symbol
//  // hedge funds,
//  //nogrouping
//  // units, description, ccy, account, investment, estimated nav, estimated net asset, estimated net asset value + post evaluation capital activity, assetclass, estimated net assetvalue, post valuation capital activity, isin, valuation date.
//  // private equities,
//  //cusip??
//  //description, account, ccy, capital commitment, capital contribution(inside commitment), estimated net asset, distributions, assetclass, capital contribution (outside commitment), estimated_net_asset_value + post val capt act + distribution, isin, outstanding commitment, recallable distribution, post valuation capital activity
//  // real estate,
//  // no grouping
//  //description(cusip, name), account, ccy, recallable distribution, isin, asset class, capital commitment, capital contributions (inside commitment), capital contribution fund expenses(outside commitment), distributions, estimated asset value +
//  // other assets,
//  // no grouping.
//  //units, description (total, per anum due), ccy (currency), isin, cusip, next pay date, account, principal income, purchaase date, region , symbol, yield, asset class, accrued interest, average unit cost, country of issuance, current value, estimated annual income, market price,total cost basis, unrealised gain(loss)
//  // liabilities,
//  //no grouping..
//  // description (account number, type), ccy(currency), amount you owe, interest rate, next payment amount, next due date, amount paid ytd, accrued amount, last payment date, last payment received, maturity date
//
//
//  test("should aggregate total amount based on asset type") {
//    new PositionsTestDataGenerator(hbaseTestUtility)
//      .createTable()
//      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
//      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI13")
//      .seedData("10100002899999", "20-Aug-14", "MONEYMAREKTMF")
//      .seedData("10100002899999", "20-Aug-14", "DCANUSNFI14")
//      .seedData("10100002899999", "21-Aug-14", "DCANUSNFI14")
//      .seedData("10100002899999", "19-Aug-14", "DCANUSNFI14")
//
//    val positions = new PositionRepository(hbaseTestUtility.getConnection).getPositionsFor("10100002899999", "assetClassCd")
//
//    assert(positions.size == 1)
//    assert(positions(0).acctKey == "10100002899999")
//  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }
}

