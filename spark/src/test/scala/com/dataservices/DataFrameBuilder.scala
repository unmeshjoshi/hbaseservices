package com.dataservices

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}


class DataFrameBuilder(schema:List[StructField]) {
  def createDataFrame(session: SparkSession, rowValues: Seq[(Int, String)]): DataFrame = {
    val rowRDD = session.sparkContext.parallelize(rows(rowValues))
    session.createDataFrame(rowRDD, StructType(schema))
  }

  private def rows(rowValues: Seq[(Int, String)]) = {
    rowValues.map(tuple ⇒ Row(tuple._1, tuple._2))
  }
}

class DataFrameBuilderS(schema:List[StructField]) {
  def createDataFrame(session: SparkSession, rowValues: Seq[String]): DataFrame = {
    val rowRDD = session.sparkContext.parallelize(rows(rowValues), 5)
    session.createDataFrame(rowRDD, StructType(schema))
  }

  private def rows(rowValues: Seq[String]) = {
    rowValues.map(value ⇒ Row(value))
  }
}