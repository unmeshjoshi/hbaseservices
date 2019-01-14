package com.hbaseservices.spark

import org.apache.hadoop.conf.Configuration

case class HbaseConnectionProperties(val zookeerQuorum:String, val zookeeperClientPort:Int)

object HbaseConnectionProperties {
  def apply(conf:Configuration) = {
    val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"
    new HbaseConnectionProperties(conf.get(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM), conf.getInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 0))
  }
}
