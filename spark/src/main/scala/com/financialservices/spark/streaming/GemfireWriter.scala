package com.financialservices.spark.streaming

import com.gemfire.{GemfireCacheProvider, PositionCache}
import com.financialservices.AccountPosition
import com.financialservices.spark.{HBaseRepository, HbaseConnectionProperties}
import org.apache.spark.sql.{DataFrame, ForeachWriter, SparkSession}


class GemfireWriter(sparkSession:SparkSession, cacheProvider:GemfireCacheProvider, zookeeperConnection: HbaseConnectionProperties) extends ForeachWriter[AccountPosition] with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  private val positionCache = new PositionCache(cacheProvider)

  override def process(position: AccountPosition): Unit = {
    val hbaseRepository = new HBaseRepository(sparkSession, zookeeperConnection)
    val dataFrame: DataFrame = hbaseRepository.readFromHBase("10100002899999")
    dataFrame.foreach(row ⇒ {
      println(s"-----------------------------------key=>value ${position}")

      positionCache.add(position)
    })
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}

