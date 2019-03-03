package com.dataservices

import com.financialservices.spark.HbaseConnectionProperties
import com.financialservices.spark.streaming.DataPipelineTestBase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}

class HbaseTableSplitTest extends DataPipelineTestBase {

  test("should get partitions for HBase table and execute scan query against each to get results") {

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
      .seedData("10100002899999", "19-Aug-14", "100")
      .seedData("10100002899999", "20-Aug-15", "110")
      .seedData("10100002899999", "21-Aug-15", "100")

    val conf = getHBaseConfiguration

    val splitsAndValues = new TableInputFormatExecutor().queryOnSplits(conf, buildScan(Map(("balance", "100")), conf))

    assert(20 == splitsAndValues._1.size)
    assert(2 == splitsAndValues._2.size)
  }


  private def buildScan(filterColumnValues: Map[String, Any], conf: Configuration) = {
    val scan = new Scan()
    scan.setCaching(100)
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterColumnValues.foreach(tuple ⇒ {
      val valueBytes = tuple._2 match {
        case str: String ⇒ Bytes.toBytes(str)
        case number: Long ⇒ Bytes.toBytes(number)
      }
      filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes(tuple._1), CompareOp.EQUAL, valueBytes))
    })
    scan.setFilter(filterList)
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
