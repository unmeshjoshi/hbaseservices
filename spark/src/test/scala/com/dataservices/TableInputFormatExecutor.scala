package com.dataservices

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.{InputSplit, JobID, RecordReader, TaskAttemptID}

import scala.collection.JavaConverters._


class TableInputFormatExecutor {

  def queryOnSplits(conf: Configuration, scan:Scan): (Seq[InputSplit], List[Result]) = {
    serializeAndSetScan(conf, scan)

    val inputFormat = new TableInputFormat()
    inputFormat.setConf(conf)

    val jobId = newJobId
    val allRowSplits = inputFormat.getSplits(new JobContextImpl(conf, jobId)).asScala.toList


    var allValues = List[Result]()
    allRowSplits.foreach(split ⇒ {
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID())
      val _reader = inputFormat.createRecordReader(
        split, hadoopAttemptContext)
      _reader.initialize(split, hadoopAttemptContext)
      allValues = (allValues ::: getAllValues(_reader))
    })
    (allRowSplits, allValues)
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

  private def serializeAndSetScan(conf: Configuration, scan: Scan) = {
    val protobufScan = ProtobufUtil.toScan(scan)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(protobufScan.toByteArray))
    conf
  }
}