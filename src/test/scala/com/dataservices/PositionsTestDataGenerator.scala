package com.dataservices

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}

class PositionsTestDataGenerator(hbaseTestUtility: HBaseTestingUtility, val columnFamily: String = "cf", val tableName: String = "positions") {
//
//  def seedData(acctKey: String, valueAsOfDate: String, assetClassCode: String) = {
//    putRow(acctKey, valueAsOfDate, assetClassCode)
//    this
//  }
//
//  private def putRow(acctKey: String, valueAsOfDate: String, accetClassCd: String) = {
//    val relationshipKey = "10500000746112"
//    val egKey = "10300000692192"
//    val alKey = "1000566819412"
//    val nomCcyCd = "USD"
//    val refCcyCd = "USD"
//    val marketUnitPriceAmount = "102.477"
//    val marketPriceDate = "19-Aug-14"
//    val totalAmount = "1739764.00"
//    val nomUnit = "1695000"
//    val nomAmount = "1736985"
//    val nomAccrInterest = "2783"
//
//
//    val p = new Put(Bytes.toBytes(s"${acctKey}-${valueAsOfDate}-${accetClassCd}"))
//    addColumn(p, columnFamily, "relationshipKey", relationshipKey)
//    addColumn(p, columnFamily, "egKey", egKey)
//    addColumn(p, columnFamily, "alKey", alKey)
//    addColumn(p, columnFamily, "nomCcyCd", nomCcyCd)
//    addColumn(p, columnFamily, "refCcyCd", refCcyCd)
//    addColumn(p, columnFamily, "marketUnitPriceAmount", marketUnitPriceAmount)
//    addColumn(p, columnFamily, "marketPriceDate", marketPriceDate)
//    addColumn(p, columnFamily, "totalAmount", totalAmount)
//    addColumn(p, columnFamily, "nomUnit", nomUnit)
//    addColumn(p, columnFamily, "nomAmount", nomAmount)
//    addColumn(p, columnFamily, "nomAccrInterest", nomAccrInterest)
//
//    val c = {
//      val configuration = hbaseTestUtility.getConfiguration
//      ConnectionFactory.createConnection(configuration)
//    }
//    c.getTable(TableName.valueOf(tableName)).put(p)
//
//    val g = new Get(Bytes.toBytes(s"${acctKey}-${valueAsOfDate}-${accetClassCd}"))
//    val result = c.getTable(TableName.valueOf(tableName)).get(g)
//    result.rawCells()(0).getTimestamp
//  }
//
//  private def addColumn(p: Put, columnFamily: String, columnName: String, columnValue: String) = {
//    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
//  }
//
//  def createTable() = {
//    val table = TableName.valueOf(tableName)
//    if (!hbaseTestUtility.getAdmin.tableExists(table))
//      createTableFor(table)
//    this
//  }
//
//  private def createTableFor(table: TableName) = {
//    val cf = ColumnFamilyDescriptorBuilder.of(columnFamily)
//    val desc = TableDescriptorBuilder.newBuilder(table)
//      .setColumnFamily(cf)
//    val tableDescriptor: TableDescriptor = desc.build()
//    hbaseTestUtility.getAdmin.createTable(tableDescriptor)
//    this
//  }
}

