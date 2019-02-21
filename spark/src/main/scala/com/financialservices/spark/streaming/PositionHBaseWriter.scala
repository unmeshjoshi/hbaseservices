package com.financialservices.spark.streaming

import com.financialservices.AccountPosition
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.spark.sql.{ForeachWriter, SparkSession}


class PositionHBaseWriter(sparkSession: SparkSession, zookeeperConnection: HbaseConnectionProperties, shouldFail:Boolean) extends ForeachWriter[AccountPosition] with Serializable {
  var exception:Exception = _
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: AccountPosition): Unit = {
    println(s"*****************************key=>value ${value}")
    new HBaseRepository(sparkSession, zookeeperConnection).putRow(value.acctKey, value.date, value.balance)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}
