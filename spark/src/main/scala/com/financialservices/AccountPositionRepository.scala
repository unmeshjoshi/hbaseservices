package com.financialservices

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

class AccountPositionRepository(val connection: Connection) {
  val columnFamily = "cf"

  def getAllPositionsFor(acctKey: String): List[AccountPosition] = {
    val scan: Scan = buildScanner(acctKey)
    val scanResult = executeScan(scan)
    val positions = getPositions(acctKey, scanResult)
    positions.toList
  }

  def getPositionsFor(acctKey: String, date:String): List[AccountPosition] = {
    val scan: Scan = buildScanner(acctKey, date)
    val scanResult = executeScan(scan)
    val positions = getPositions(acctKey, scanResult)
    positions
  }

  private def getPositions(acctKey: String, scanner: ResultScanner) = {
    scanner.toList.map(result ⇒ {
      val balance = getValue(result, "balance")
      val rowKey = Bytes.toString(result.getRow)
      AccountPosition(acctKey, balance, rowKey)
    })
  }

  private def getValue(result: Result, columnName: String) = {
    val cells = result.rawCells()
    cells.foreach(cell ⇒ {
      val values = cell.getValue()
      println(new String(values))
    })
    Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)))
  }

  private def executeScan(scan: Scan) = {
    val table = connection.getTable(TableName.valueOf("Positions"))
    val scanner: ResultScanner = table.getScanner(scan)
    scanner
  }

  private def buildScanner(acctKey: String) = {
    val acctKeyBytes = Bytes.toBytes(acctKey)
    val scan = new Scan()
    scan.setFilter(new PrefixFilter(acctKeyBytes))
    scan.setMaxVersions(10)
    scan
  }
  private def buildScanner(acctKey: String, date:String) = {
    val rowKeyPrefix = s"${acctKey}_${date}"
    println(s"Fetching records maching prefix ${rowKeyPrefix}")
    val scan = new Scan()
    scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKeyPrefix)))
    scan.setMaxVersions(10)
    scan
  }
}