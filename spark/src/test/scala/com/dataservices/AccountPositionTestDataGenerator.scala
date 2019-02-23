package com.dataservices

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

import scala.util.Random

class AccountPositionTestDataGenerator(connection:Connection, val columnFamily: String = "cf", val tableName: String = "Positions") {
  def deleteTable(): Unit = {
    val admin = connection.getAdmin
    admin.deleteTable(TableName.valueOf(tableName))
  }

  def seedData(acctKey: String, date: String, balance:String, units:String = "0", versionNumber:Long = Long.MaxValue): AccountPositionTestDataGenerator = {
    val t1 = putRow(acctKey, date, balance, units, versionNumber, uniqueRowKey(acctKey, date))
    println(t1)
    this
  }

  private def uniqueRowKey(acctKey: String, date: String) = {
    s"${acctKey}_${date}_${new Random().nextInt()}"
  }


  def seedData(acctKey: String, date: String, balance:String, versionNumber:Long, rowKey:String): AccountPositionTestDataGenerator = {
    val t1 = putRow(acctKey, date, balance, "0", versionNumber, rowKey)
    println(t1)
    this
  }

  private def putRow(acctKey: String, date: String, balance:String, units:String, version:Long, rowKey:String) = {
     put(rowKey, putRequest(rowKey, balance, units, version))
  }

  private def put(rowKey:String, p: Put) = {
    val hbaseTable: Table = getHBaseTable(tableName)
    hbaseTable.put(p)
    getLatestVersionTimestamp(rowKey, hbaseTable)
  }

  private def getHBaseTable(tableName: String) = {
    val hbaseTable = connection.getTable(TableName.valueOf(tableName))
    hbaseTable
  }

  private def putRequest(hbaseKey: String, balance: String, units:String, version:Long) = {
    val p = new Put(Bytes.toBytes(hbaseKey))
    addColumn(p, columnFamily, "balance", balance, version)
    addColumn(p, columnFamily, "units", balance, version)
    p
  }

  private def getLatestVersionTimestamp(hbaseKey: String, hbaseTable: Table) = {
    val g = new Get(Bytes.toBytes(hbaseKey))
    g.setMaxVersions(40)
    val result = hbaseTable.get(g)

    result.rawCells()(0).getTimestamp
  }

  private def addColumn(p: Put, columnFamily: String, columnName: String, columnValue: String, version:Long) = {
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), version, Bytes.toBytes(columnValue))
  }


  def createTable() = {
    val noOfVersions: Int = 40
    val admin = connection.getAdmin
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      println(s"Creating table ${tableName}")
      val desc = new HTableDescriptor(tableName)
      desc.addFamily(new HColumnDescriptor("cf"))
      admin.createTable(desc)
    }

    this
  }

}

