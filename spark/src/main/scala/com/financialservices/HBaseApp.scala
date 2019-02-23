package com.financialservices

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}

object HBaseApp extends App {
  val config: Configuration = HBaseConfiguration.create()
  config.set("hbase.zookeeper.quorum", "192.168.56.101")
  config.setInt("hbase.zookeeper.property.clientPort", 2181)
  config.set(TableInputFormat.INPUT_TABLE, "Positions")
  config.set(TableOutputFormat.OUTPUT_TABLE, "Positions")
  config.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

  import org.apache.hadoop.hbase.client.ConnectionFactory

  val connection = ConnectionFactory.createConnection(config)




}
