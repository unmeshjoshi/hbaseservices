package com.dataservices

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}

class AccountPositionTestDataGenerator(hbaseTestUtility: HBaseTestingUtility, val columnFamily: String = "cf", val tableName: String = "Positions") {

  def seedData(acctKey: String, date: String, balance:String): AccountPositionTestDataGenerator = {
    val t1 = putRow(acctKey, date, balance)
    println(t1)
    this
  }

  private def putRow(acctKey: String, date: String, balance:String) = {
    val p = new Put(Bytes.toBytes(s"${acctKey}-${date}"))
    addColumn(p, columnFamily, "balance", balance)

    val c: Connection = hbaseTestUtility.getConnection
    val hbaseTable = c.getTable(TableName.valueOf(tableName))

    hbaseTable.put(p)

    val g = new Get(Bytes.toBytes(s"${acctKey}-${date}"))
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

