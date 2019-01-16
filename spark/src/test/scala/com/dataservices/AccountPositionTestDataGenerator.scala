package com.dataservices

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}

import scala.util.Random

class AccountPositionTestDataGenerator(hbaseTestUtility: HBaseTestingUtility, val columnFamily: String = "cf", val tableName: String = "Positions") {
  def deleteTable(): Unit = {
    val table = hbaseTestUtility.deleteTable(TableName.valueOf(tableName))
  }

  def seedData(acctKey: String, date: String, balance:String): AccountPositionTestDataGenerator = {
    val t1 = putRow(acctKey, date, balance)
    println(t1)
    this
  }

  private def putRow(acctKey: String, date: String, balance:String) = {
    val rowKey: String = uniqueRowKey(acctKey, date)
    put(rowKey, putRequest(rowKey, balance))
  }

  private def put(rowKey:String, p: Put) = {
    val hbaseTable: Table = getHBaseTable(tableName)
    hbaseTable.put(p)
    getLatestVersionTimestamp(rowKey, hbaseTable)
  }

  private def getHBaseTable(tableName: String) = {
    val c = hbaseTestUtility.getConnection
    val hbaseTable = c.getTable(TableName.valueOf(tableName))
    hbaseTable
  }

  private def putRequest(hbaseKey: String, balance: String) = {
    val p = new Put(Bytes.toBytes(hbaseKey))
    addColumn(p, columnFamily, "balance", balance)
    p
  }

  private def uniqueRowKey(acctKey: String, date: String) = {
    s"${acctKey}_${date}_${new Random().nextInt()}"
  }

  private def getLatestVersionTimestamp(hbaseKey: String, hbaseTable: Table) = {
    val g = new Get(Bytes.toBytes(hbaseKey))
    g.setMaxVersions(40)
    val result = hbaseTable.get(g)

    result.rawCells()(0).getTimestamp
  }

  private def addColumn(p: Put, columnFamily: String, columnName: String, columnValue: String) = {
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
  }


  def createTable() = {
    val noOfVersions: Int = 40
    val table = hbaseTestUtility.createTable(TableName.valueOf(tableName), Bytes.toBytes(columnFamily), noOfVersions)
    this
  }

}

