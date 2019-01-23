package com.dataservices

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkDataframeTest extends FunSuite with BeforeAndAfterAll with Matchers with Eventually {
  test("Should give differences in two dataframes") {
    val sparkConf = new SparkConf().setAppName("spark-dataframe-tests").setMaster("local[*]")

    val session = SparkSession
      .builder
      .config(sparkConf).getOrCreate()

    val df1 = createDataFrame(session, Seq((1, "ABC"), (2, "XYZ")))
    val df2 = createDataFrame(session, Seq((2, "XYZ"), (3, "LMN")))

    val diffDf = df1.except(df2).union(df2.except(df1))

    val diffRows = diffDf.collect()
    val expectedRows = rows(Seq((1, "ABC"), (3, "LMN"))).toArray
    diffRows shouldEqual expectedRows
  }

  def createDataFrame(session: SparkSession, rowValues: Seq[(Int, String)]): DataFrame = {
    val schema = List(StructField("number", IntegerType, true), StructField("text", StringType, true))
    val rowRDD = session.sparkContext.parallelize(rows(rowValues))
    session.createDataFrame(rowRDD, StructType(schema))
  }

  private def rows(rowValues: Seq[(Int, String)]) = {
    rowValues.map(tuple ⇒ Row(tuple._1, tuple._2))
  }
}
