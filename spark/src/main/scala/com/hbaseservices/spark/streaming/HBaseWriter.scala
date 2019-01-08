package com.hbaseservices.spark.streaming

import com.hbaseservices.Position
import com.hbaseservices.spark.HBaseRepository
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{ForeachWriter, SparkSession}


class HBaseWriter(sparkSession: SparkSession, zookeeperQuorum: String, hbaseZookeeperClientPort: Int, hbaseTableName: String) extends ForeachWriter[Position] with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Position): Unit = {
    println(s"*****************************key=>value ${value}")
    new HBaseRepository(sparkSession, "cf", zookeeperQuorum, hbaseZookeeperClientPort, hbaseTableName).putRow(value.acctKey, value.valueAsOfDate, value.assetClassCd)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}
