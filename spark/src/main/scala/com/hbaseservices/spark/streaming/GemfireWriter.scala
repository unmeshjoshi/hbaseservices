package com.hbaseservices.spark.streaming

import com.gemfire.{GemfireCacheProvider, PositionCache}
import com.hbaseservices.Position
import org.apache.spark.sql.{ForeachWriter, SparkSession}


class GemfireWriter(sparkSession:SparkSession, cacheProvider:GemfireCacheProvider) extends ForeachWriter[Position] with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  private val positionCache = new PositionCache(cacheProvider)

  override def process(position: Position): Unit = {
    println(s"*****************************key=>value ${position}")
    positionCache.add(position)
  }

  override def close(errorOrNull: Throwable): Unit = {
    //noop
  }
}

