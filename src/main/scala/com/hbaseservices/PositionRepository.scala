package com.hbaseservices

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

class PositionRepository(val connection: Connection) {
  val columnFamily = "cf"

  def getPositionsFor(acctKey: String, aggregateByField:String): List[Position] = {
    List[Position]()
  }

  def getPositionsFor(acctKey: String): List[Position] = {
    val key = Bytes.toBytes(s"${acctKey}-19-Aug-14-MONEYMAREKTMF")
    val g = new Get(key)
    val table = connection.getTable(TableName.valueOf("positions"))
    val result = table.get(g)


    val scan: Scan = buildScanner(acctKey)
    val scanResult = executeScan(scan)
    val positions = getPositions(acctKey, scanResult)

    positions.toList
  }

  private def getPositions(acctKey: String, scanner: ResultScanner) = {
    scanner.toList.map(result ⇒ {
      val relKey = getValue(result, "relationshipKey")
      val egKey = getValue(result, "egKey")

      //TBD. Add following attributes to model
      getValue(result, "alKey")
      getValue(result, "nomCcyCd")
      getValue(result, "refCcyCd")
      val marketUnitPriceAmount = getValue(result, "marketUnitPriceAmount")
      val marketPriceDate = getValue(result, "marketPriceDate")
      getValue(result, "totalAmount")
      getValue(result, "nomUnit")
      getValue(result, "nomAmount")
      getValue(result, "nomAccrInterest")

      Position(acctKey, egKey, marketUnitPriceAmount, marketPriceDate)

    })
  }

  private def getValue(result: Result, columnName: String) = {
    result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)).toString
  }

  private def executeScan(scan: Scan) = {
    val table = connection.getTable(TableName.valueOf("positions"))
    val scanner: ResultScanner = table.getScanner(scan)
    scanner
  }

  private def buildScanner(acctKey: String) = {
    val acctKeyBytes = Bytes.toBytes(acctKey)
    val scan = new Scan(acctKeyBytes)
    scan
  }
}