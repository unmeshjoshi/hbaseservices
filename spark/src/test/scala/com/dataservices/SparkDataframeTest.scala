package com.dataservices


import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkDataframeTest extends  FunSuite with BeforeAndAfterAll with Matchers with Eventually {


  test("should load csv dataframe") {
    val sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]")
    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")

    val session = SparkSession
      .builder
      .config(sparkConf).getOrCreate()
    // Set Spark Configuration// Set Spark Configuration


    val path = getClass.getClassLoader().getResource("LoanStats3a.csv").getPath
    val loanStatsDataset = session.read
      .option("header", "true")
      .option("inferSchema","true")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .csv(path)

    System.out.println("Printing DataFrame in readDataFromFile")
    val dataFrame = loanStatsDataset.toDF().groupBy("member_id").count()
    dataFrame.explain(extended = true)
    val rows = dataFrame.take(100)
    rows.foreach(row ⇒ {
      val size = row.size
      var i = 0
      while(i < size) {
        print(row.get(i))
        i = i + 1
      }
      println()
    })



  }
}
