import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val HbaseService = Seq(
    Libs.`guava`,
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    HBase.`hbase-client`,
    HBase.TestOnly.`hbase-test-utils` ,
    Spark.`spark-core`,
    Spark.`spark-sql`,
    Spark.`avro`,
    Spark.`spark-catalyst-test`,
    Spark.`spark-core-test`,
    Spark.`spark-sql-test`,
    Libs.`geode`
  )
}