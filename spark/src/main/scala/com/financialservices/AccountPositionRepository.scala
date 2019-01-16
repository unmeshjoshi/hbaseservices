package com.financialservices

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

class AccountPositionRepository(val connection: Connection) {
  val columnFamily = "cf"

  def getPositionsFor(acctKey: String, aggregateByField:String): List[AccountPosition] = {
    List[AccountPosition]()
  }

  val date = "19-Aug-14"
  def getPositionsFor(acctKey: String): List[AccountPosition] = {
    val key = Bytes.toBytes(s"${acctKey}-$date")
    val g = new Get(key)
    g.setMaxVersions()
    val table = connection.getTable(TableName.valueOf("Positions"))
    val result = table.get(g)


    val scan: Scan = buildScanner(acctKey)
    val scanResult = executeScan(scan)
    val positions = getPositions(acctKey, scanResult, date)

    positions.toList
  }

  private def getPositions(acctKey: String, scanner: ResultScanner, date:String) = {
    scanner.toList.map(result ⇒ {
      val balance = getValue(result, "balance")
      AccountPosition(acctKey, balance, date)

    })
  }

  private def getValue(result: Result, columnName: String) = {
    val cells = result.rawCells()
    cells.foreach(cell ⇒ {
      val values = cell.getValue()
      println(new String(values))
    })
    result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)).toString
  }

  private def executeScan(scan: Scan) = {
    val table = connection.getTable(TableName.valueOf("Positions"))
    val scanner: ResultScanner = table.getScanner(scan)
    scanner
  }

  private def buildScanner(acctKey: String) = {
    val acctKeyBytes = Bytes.toBytes(acctKey)
    val scan = new Scan(acctKeyBytes)
    scan.setMaxVersions(10)
    scan
  }
}