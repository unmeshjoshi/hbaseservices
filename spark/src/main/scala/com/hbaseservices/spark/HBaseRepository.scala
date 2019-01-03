package com.hbaseservices.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class HBaseRepository extends Serializable {
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

  def readFromHBase(sparkSession:SparkSession, conf:Configuration) = {
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

      getRow(result, hbaseSchema, "cf")
    })
    sparkSession.createDataFrame(rowRDD, hbaseSchema)
  }
}
