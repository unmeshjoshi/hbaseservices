package com.financialservices.spark

import org.apache.hadoop.conf.Configuration

case class HbaseConnectionProperties(val zookeerQuorum:String, val zookeeperClientPort:Int)

object HbaseConnectionProperties {
  def apply(conf:Configuration) = {
    new HbaseConnectionProperties(conf.get("hbase.zookeeper.quorum"), conf.getInt("hbase.zookeeper.property.clientPort", 0))
  }
}
