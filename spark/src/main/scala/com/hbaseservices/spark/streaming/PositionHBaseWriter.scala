package com.hbaseservices.spark.streaming

import com.hbaseservices.AccountPosition
import com.hbaseservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.spark.sql.{ForeachWriter, SparkSession}


class PositionHBaseWriter(sparkSession: SparkSession, zookeeperConnection: HbaseConnectionProperties) extends ForeachWriter[AccountPosition] with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: AccountPosition): Unit = {
    println(s"*****************************key=>value ${value}")
    new HBaseRepository(sparkSession, zookeeperConnection).putRow(value.acctKey, value.date, value.balance)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}
