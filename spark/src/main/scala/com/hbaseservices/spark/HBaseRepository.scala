package com.hbaseservices.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

class HBaseRepository(sparkSession: SparkSession, @transient conf: Configuration, columnFamily:String) extends Serializable {
  val hbaseSchema: StructType = StructType(List(StructField("egKey", DataTypes.StringType, true, Metadata.empty),
    StructField("alKey", DataTypes.StringType, true, Metadata.empty),
    StructField("nomCcyCd", DataTypes.StringType, true, Metadata.empty),
    StructField("refCcyCd", DataTypes.StringType, true, Metadata.empty),
    StructField("marketUnitPriceAmount", DataTypes.StringType, true, Metadata.empty),
    StructField("marketPriceDate", DataTypes.StringType, true, Metadata.empty),
    StructField("totalAmount", DataTypes.StringType, true, Metadata.empty),
    StructField("nomUnit", DataTypes.StringType, true, Metadata.empty),
    StructField("nomAmount", DataTypes.StringType, true, Metadata.empty),
    StructField("nomAccrInterest", DataTypes.StringType, true, Metadata.empty),
    StructField("relationshipKey", DataTypes.StringType, true, Metadata.empty)))

  def readFromHBase() = {
    val hBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = hBaseRDD.map(tuple ⇒ tuple._2)
    val rowRDD = resultRDD.map(result ⇒ {
      def getRow(result: Result, schema: StructType, columnFamily: String): Row = {
        val values = schema.fields.map(field ⇒ {
          val dataType: DataType = field.dataType
          val value = result.getValue(columnFamily.getBytes, field.name.getBytes)
          dataType match {
            case DataTypes.LongType ⇒ Bytes.toLong(value)
            case DataTypes.StringType ⇒ Bytes.toString(value)
          }
        })
        new GenericRowWithSchema(values, schema)
      }

      getRow(result, hbaseSchema, columnFamily)
    })
    sparkSession.createDataFrame(rowRDD, hbaseSchema)
  }


  def writeToHBase(acctKey: String, valueAsOfDate: String, accetClassCd: String) = {
    val egKey = "10300000692192"
    val alKey = "1000566819412"
    val nomCcyCd = "USD"
    val refCcyCd = "USD"
    val marketUnitPriceAmount = "102.477"
    val marketPriceDate = "19-Aug-14"
    val totalAmount = "1739764.00"
    val nomUnit = "1695000"
    val nomAmount = "1736985"
    val nomAccrInterest = "2783"
    val relationshipKey = "10500000746112"
    val rows: Seq[Row] = List(Row(egKey, alKey, nomCcyCd, refCcyCd, marketUnitPriceAmount, marketPriceDate, totalAmount, nomUnit, nomAmount, nomAccrInterest, relationshipKey))


    val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), hbaseSchema)
    dataFrame.rdd.map(row ⇒ {
      val put = new Put(Bytes.toBytes(s"${acctKey}-${valueAsOfDate}-${accetClassCd}"))
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
}
