package com.financialservices.spark

import java.util.Random

import com.financialservices.AccountPositionRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, PrefixFilter, RowFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer


class HBaseRepository(sparkSession: SparkSession, zookeeperQuorum: HbaseConnectionProperties) extends Serializable {
  val columnFamily = "cf"
  val tableName = "Positions"
  val HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
  val HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort"

  import org.apache.hadoop.hbase.HBaseConfiguration
  import org.apache.hadoop.hbase.client.ConnectionFactory

  @transient val conf = getHbaseConfiguration()

  def getHbaseConfiguration() = {
    val conf = HBaseConfiguration.create()
    conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, zookeeperQuorum.zookeerQuorum)
    conf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, zookeeperQuorum.zookeeperClientPort)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf
  }

  val hbaseSchema: StructType = StructType(List(StructField("balance", DataTypes.StringType, true, Metadata.empty)))

  def readFromHBase(df: DataFrame) = {
      import sparkSession.implicits._
     df.mapPartitions(rows ⇒ {
       val hbaseConnection = ConnectionFactory.createConnection(getHbaseConfiguration())
       val repo = new AccountPositionRepository(hbaseConnection)
       val list = rows.toList
       val table = hbaseConnection.getTable(TableName.valueOf("Positions"))
       val scan = new Scan()
       scan.setStartRow(Bytes.toBytes(list(0).get(0).asInstanceOf[String]))
       scan.setStopRow(Bytes.toBytes(list(list.length - 1).get(0).asInstanceOf[String]))
       val scanner = table.getScanner(scan)
       val positions = new AccountPositionRepository(hbaseConnection).getPositions(scanner)
       positions.toIterator
     })
  }


  def readFromHBase(filterColumnValues: Map[String, Any]) = {
    val hbaseConf = setScan(filterColumnValues, conf)
    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = hBaseRDD.map(tuple ⇒ tuple._2)
    val rowRDD = resultRDD.map(result ⇒ {
      getRow(result, hbaseSchema, columnFamily)
    })
    sparkSession.createDataFrame(rowRDD, hbaseSchema)
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
      filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(tuple._1), CompareOp.EQUAL, valueBytes))
    })
    scan.setFilter(filterList)

    setScan(conf, scan)
  }

  private def setScan(conf: Configuration, scan: Scan) = {
    val protobufScan = ProtobufUtil.toScan(scan)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(protobufScan.toByteArray))
    conf
  }

  private def getRow(result: Result, schema: StructType, columnFamily: String): Row = {
    val values = schema.fields.map(field ⇒ {
      val dataType: DataType = field.dataType
      val value = result.getValue(columnFamily.getBytes, field.name.getBytes)
      val cells = result.rawCells()
      dataType match {
        case DataTypes.LongType ⇒ Bytes.toLong(value)
        case DataTypes.StringType ⇒ Bytes.toString(value)
      }
    })
    new GenericRowWithSchema(values, schema)
  }

  def readFromHBase(keyPrefix: String) = {
    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions()
    scan.setFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)))
    val hbaseConf = setScan(conf, scan)
    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = hBaseRDD.map(tuple ⇒ tuple._2)
    val rowRDD = resultRDD.map(result ⇒ {
      getRow(result, hbaseSchema, columnFamily)
    })
    sparkSession.createDataFrame(rowRDD, hbaseSchema)
  }

  def writeToHBase(acctKey: String, valueAsOfDate: String, balance: String) = {
    val rows: immutable.Seq[Row] = (200 to 300).map(i ⇒ {
        Row(s"${i}")
    })



    val dataFrame: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), hbaseSchema)
    dataFrame.rdd.map((row: Row) ⇒ {
      val put = new Put(Bytes.toBytes(s"${acctKey}-${valueAsOfDate}-" + new Random().nextInt()))
      hbaseSchema.fields.foreach(field ⇒ {
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field.name), getValue(row, field))
      })
      (new ImmutableBytesWritable(), put)
    }).saveAsNewAPIHadoopDataset(conf)
  }

  def getValue(row: Row, field: StructField): Array[Byte] = {
    field.dataType match {
      case DataTypes.StringType ⇒ Bytes.toBytes(row.getAs[String](field.name))
      case DataTypes.LongType ⇒ Bytes.toBytes(row.getAs[Long](field.name))
      case _ ⇒ throw new IllegalArgumentException(s"${field.dataType} not supported for field ${field.name}")
    }
  }

  def putRow(acctKey: String, valueAsOfDate: String, balance: String) = {

    val p = new Put(Bytes.toBytes(s"${acctKey}-${valueAsOfDate}"))
    addColumn(p, columnFamily, "balance", balance)

    val connection = ConnectionFactory.createConnection(conf)

    val hbaseTable = connection.getTable(TableName.valueOf(tableName))
    hbaseTable.put(p)
    hbaseTable.close()
  }

  def addColumn(p: Put, columnFamily: String, columnName: String, columnValue: String) = {
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
  }

}
