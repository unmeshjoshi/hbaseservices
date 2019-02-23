package com.dataservices

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes

object HBaseApp extends App {
  val config: Configuration = HBaseConfiguration.create()
  config.set("hbase.zookeeper.quorum", "192.168.56.101")
  config.setInt("hbase.zookeeper.property.clientPort", 2181)
  config.set(TableInputFormat.INPUT_TABLE, "Positions")
  config.set(TableOutputFormat.OUTPUT_TABLE, "Positions")
  config.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

  import org.apache.hadoop.hbase.client.ConnectionFactory

  val connection = ConnectionFactory.createConnection(config)
  private val generator: AccountPositionTestDataGenerator = new AccountPositionTestDataGenerator(connection).createTable()
  (1 to 1000).foreach(i ⇒ {
    val accountNumberBase = "101000028999"
    val accountKey = s"${accountNumberBase}${i}"
    generator
      .seedData(accountKey, "19-Aug-14", "100")
  })

  val splitsAndValues = new TableInputFormatExecutor().queryOnSplits(config, buildScan(Map(("balance", "100")), config))

  assert(1 == splitsAndValues._1.size)
  assert(2 == splitsAndValues._2.size)


  private def buildScan(filterColumnValues: Map[String, Any], conf: Configuration) = {
    val scan = new Scan()
    scan.setCaching(100)
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterColumnValues.foreach(tuple ⇒ {
      val valueBytes = tuple._2 match {
        case str: String ⇒ Bytes.toBytes(str)
        case number: Long ⇒ Bytes.toBytes(number)
      }
      filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes(tuple._1), CompareOp.EQUAL, valueBytes))
    })
    scan.setFilter(filterList)
  }
}

