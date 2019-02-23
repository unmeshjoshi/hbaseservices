package com.dataservices

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.financialservices.spark.HbaseConnectionProperties
import com.financialservices.spark.streaming.DataPipelineTestBase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.{JobID, RecordReader, TaskAttemptID}

import scala.collection.JavaConverters._

class HbaseTableSplitTest extends DataPipelineTestBase {
  test("should get partitions for HBase table and execute scan query against each to get results") {

    val positionGenerator = new AccountPositionTestDataGenerator(hbaseTestUtility.getConnection).createTable()
      .seedData("10100002899999", "19-Aug-14", "100")
      .seedData("10100002899999", "20-Aug-15", "110")
      .seedData("10100002899999", "21-Aug-15", "100")

    val conf = getHBaseConfiguration
    setScan(Map(("balance", "100")), conf)

    val inputFormat = new TableInputFormat()
    inputFormat.setConf(conf)

    val jobId = newJobId
    val allRowSplits = inputFormat.getSplits(new JobContextImpl(conf, jobId)).asScala

    assert(1 == allRowSplits.size)

    var allValues = List[Result]()
    allRowSplits.foreach(split ⇒ {
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID())
      val _reader = inputFormat.createRecordReader(
        split, hadoopAttemptContext)
      _reader.initialize(split, hadoopAttemptContext)
      allValues = (allValues ::: getAllValues(_reader))
    })

    assert(2 == allValues.size)
  }

  private def getAllValues(_reader: RecordReader[ImmutableBytesWritable, Result]) = {
    var values = List[Result]()
    while (_reader.nextKeyValue()) {
      val key = _reader.getCurrentKey
      val value = _reader.getCurrentValue
      values = values :+ value
    }
    values
  }

  private def newJobId = {
    val jobTrackerId: String = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date())
    val jobNumber = 1
    val jobId = new JobID(jobTrackerId, jobNumber)
    jobId
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


  private def setScan(filterColumnValues: Map[String, Any], conf: Configuration): Configuration = {
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

    serializeAndSetScan(conf, scan)
  }

  private def serializeAndSetScan(conf: Configuration, scan: Scan) = {
    val protobufScan = ProtobufUtil.toScan(scan)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(protobufScan.toByteArray))
    conf
  }

}
