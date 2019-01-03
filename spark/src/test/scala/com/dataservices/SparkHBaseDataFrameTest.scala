package com.dataservices

import com.hbaseservices.spark.HBaseRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkHBaseDataFrameTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {

  val hbaseTestUtility = newHbaseTestUtility

  val columnFamily: String = "cf"

  override def afterAll(): Unit = {
    hbaseTestUtility.shutdownMiniCluster()
  }



  override protected def beforeAll(): Unit = {
    hbaseTestUtility.startMiniCluster();
  }

  test("should read hbase data with spark dataframe") {
    val sparkConf = new SparkConf().setAppName("HBaseDataframe").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val positionGenerator = new PositionsTestDataGenerator(hbaseTestUtility, columnFamily).createTable()
      .seedData("10100002899999", "19-Aug-14", "MONEYMAREKTMF")
      .seedData("10100002899999", "19-Aug-15", "MONEYMAREKTMF")
    positionGenerator

    val conf: Configuration = hbaseTestUtility.getConfiguration
    conf.set(TableInputFormat.INPUT_TABLE, "positions")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "positions")

    //    writeToHBase(sc, sparkSession, conf)
    val dataFrame = new HBaseRepository().readFromHBase(sparkSession, conf)
    println("Number of Records found : " + dataFrame.count())


  }

  private def writeToHBase(sc: SparkContext, sparkSession: SparkSession, conf: Configuration) = {
//    val someData = Seq(
//      Row(8, "bat"),
//      Row(64, "mouse"),
//      Row(-27, "horse")
//    )
//
//    val someSchema = List(
//      StructField("number", IntegerType, true),
//      StructField("word", StringType, true)
//    )
//
//    val dataFrame = sparkSession(
//      sc.parallelize(someData),
//      StructType(someSchema)
//    )
//    dataFrame.rdd.map(row ⇒ (new ImmutableBytesWritable(), new Put())).saveAsNewAPIHadoopDataset(conf)
  }

  private def newHbaseTestUtility = {
    //https://jira.apache.org/jira/browse/HBASE-20544
    val config: Configuration = HBaseConfiguration.create
    config.setInt(HConstants.REGIONSERVER_PORT, 0)
    val hbaseTestUtility = new HBaseTestingUtility(config);
    hbaseTestUtility
  }

}
