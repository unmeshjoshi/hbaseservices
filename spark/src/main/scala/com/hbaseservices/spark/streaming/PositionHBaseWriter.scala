package com.hbaseservices.spark.streaming

import com.hbaseservices.Position
import com.hbaseservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.spark.sql.{ForeachWriter, SparkSession}


class PositionHBaseWriter(sparkSession: SparkSession, zookeeperConnection: HbaseConnectionProperties) extends ForeachWriter[Position] with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Position): Unit = {
    println(s"*****************************key=>value ${value}")
    new HBaseRepository(sparkSession, zookeeperConnection).putRow(value.acctKey, value.valueAsOfDate, value.assetClassCd)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}
