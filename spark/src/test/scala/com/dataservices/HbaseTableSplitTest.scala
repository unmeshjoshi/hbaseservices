package com.dataservices

import com.financialservices.spark.HbaseConnectionProperties
import com.financialservices.spark.streaming.DataPipelineTestBase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}

class HbaseTableSplitTest extends DataPipelineTestBase {
  test("should get partitions for HBase table and execute scan query against each to get results") {

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
      .seedData("10100002899999", "19-Aug-14", "100")
      .seedData("10100002899999", "20-Aug-15", "110")
      .seedData("10100002899999", "21-Aug-15", "100")

    val conf = getHBaseConfiguration

    val splitsAndValues = new TableInputFormatExecutor().queryOnSplits(conf, Map(("balance", "100")))

    assert(1 == splitsAndValues._1.size)
    assert(2 == splitsAndValues._2.size)
  }

  private def getHBaseConfiguration = {
    val conf: Configuration = hbaseTestUtility.getConfiguration

    val columnFamily = "cf"
    val tableName = "Positions"
    val zookeeperQuorum = HbaseConnectionProperties(conf)
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum.zookeerQuorum)
    conf.setInt("hbase.zookeeper.property.clientPort", zookeeperQuorum.zookeeperClientPort)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf
  }

}
