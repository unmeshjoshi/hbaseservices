import sbt._

object Dependencies {

  val Version = "0.1-SNAPSHOT"
  val HbaseService = Seq(
    Libs.`junit` % Test,
    Libs.`junit-interface` % Test,
    Libs.`mockito-core` % Test,
    Libs.`scalatest` % Test,
    HBase.hadoopCommon,
    HBase.hadoopHdfs,
    HBase.hbase,
    HBase.hbaseClient,
    HBase.hbaseTestingUtil % Test
  )
}