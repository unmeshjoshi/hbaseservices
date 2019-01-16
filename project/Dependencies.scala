import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val SqoopDependencies = Seq(
    Libs.`hsqldb` % Test,
    Libs.`guava`,  //need to put in specific version 12.0.1 which hadoop and hbase versions from cdh 5.4.14 depend on. Its excluded explicitly from all the other cdh dependencies`
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    Sqoop.`sqoop`,
    Sqoop.`kite-data-mapreduce`,
    Spark.`avro`,
    Libs.`geode`
  )
  val SparkDependencies = Seq(
    Libs.`hsqldb` % Test,
    Libs.`guava`,  //need to put in specific version 12.0.1 which hadoop and hbase versions from cdh 5.4.14 depend on. Its excluded explicitly from all the other cdh dependencies`
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    Hadoop.`hadoop-yarn-tests` % Test,
    HBase.`hbase-client`,
    HBase.`hbase-server`,
    HBase.TestOnly.`hbase-test-utils` ,
    Spark.`spark-core`,
    Spark.`spark-sql`,
    Spark.`avro`,
    Spark.`spark-catalyst-test`,
    Spark.`spark-core-test`,
    Spark.`spark-sql-test`,
    Spark.sparkStreaming,
    Spark.sparkSQL,
    Spark.sparkHiveSQL,
    Spark.sparkTestingBase % Test,
    Spark.sparkStreamingKafka,
    Spark.sparkStructuredStreamingKafka,
    //    Kafka.`akka-stream-kafka`,
    //    Kafka.`kafkaStreamsScala`,
    Kafka.`scalatest-embedded-kafka` % Test,
    Libs.`geode`,
    Libs.`xstream`
  )
}