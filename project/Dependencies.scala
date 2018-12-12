import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val HbaseService = Seq(
    Libs.`hsqldb`,
    Libs.`guava`,  //need to put in specific version 12.0.1 which hadoop and hbase versions from cdh 5.4.14 depend on. Its excluded explicitly from all the other cdh dependencies`
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    HBase.`hbase-client`,
    HBase.TestOnly.`hbase-test-utils` ,
    Sqoop.`sqoop`,
    Sqoop.`commons-io`,
    Sqoop.`commons-cli`,
    Sqoop.`commons-logging`,
    Sqoop.`log4j`,
    Hadoop.`hadoop-common`,
    Hadoop.`hadoop-hdfs`,
    Hadoop.`hadoop-auth`,
//    Hadoop.`hadoop-core`, //not found on cdh
    Hadoop.`hadoop-client`,
    Spark.`spark-core`,
    Spark.`spark-sql`,
    Spark.`avro`,
    Spark.`spark-catalyst-test`,
    Spark.`spark-core-test`,
    Spark.`spark-sql-test`,
    Libs.`geode`
  )
}