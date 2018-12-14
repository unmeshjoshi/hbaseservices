import sbt._
import scalapb.compiler.Version.scalapbVersion

object CDHVersions {
  val Hbase = "1.2.0-cdh5.14.4"
  val Hadoop = "2.6.0-cdh5.14.4"
  val Spark = "2.3.0.cloudera2"
  val Sqoop = "1.4.6-cdh5.14.4"
  val `kite-data-mapreduce` =  "1.0.0-cdh5.14.4" //required by sqoop for parquet format
}

object Libs {


  val ScalaVersion = "2.11.8"

  val `scalatest` = "org.scalatest" %% "scalatest" % "3.0.4" //Apache License 2.0
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0" //BSD 3-clause "New" or "Revised" License
  val `scala-async` = "org.scala-lang.modules" %% "scala-async" % "0.9.7" //BSD 3-clause "New" or "Revised" License
  val `scopt` = "com.github.scopt" %% "scopt" % "3.7.0" //MIT License
  val `acyclic` = "com.lihaoyi" %% "acyclic" % "0.1.7" % Provided //MIT License
  val `junit` = "junit" % "junit" % "4.12" //Eclipse Public License 1.0
  val `junit-interface` = "com.novocode" % "junit-interface" % "0.11" //BSD 2-clause "Simplified" License
  val `mockito-core` = "org.mockito" % "mockito-core" % "2.12.0" //MIT License
  val `logback-classic` = "ch.qos.logback" % "logback-classic" % "1.2.3" //Dual license: Either, Eclipse Public License v1.0 or GNU Lesser General Public License version 2.1
  val `akka-management-cluster-http` = "com.lightbend.akka" %% "akka-management-cluster-http" % "0.5" //N/A at the moment
  val svnkit = "org.tmatesoft.svnkit" % "svnkit" % "1.9.0" //TMate Open Source License
  val `commons-codec` = "commons-codec" % "commons-codec" % "1.10" //Apache 2.0
  val `persist-json` = "com.persist" %% "persist-json" % "1.2.1" //Apache 2.0
  val `joda-time` = "joda-time" % "joda-time" % "2.9.9" //Apache 2.0
  val `scala-reflect` = "org.scala-lang" % "scala-reflect" % ScalaVersion //BSD-3
  val `gson` = "com.google.code.gson" % "gson" % "2.8.2" //Apache 2.0
  val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.7" //Apache 2.0
  val `play-json-extensions` = "ai.x" %% "play-json-extensions" % "0.10.0" //Simplified BSD License
  val `akka-http-play-json` = "de.heikoseeberger" %% "akka-http-play-json" % "1.18.1" //Apache 2.0
  val `scalapb-runtime` = "com.trueaccord.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf"
  val `scalapb-json4s` = "com.trueaccord.scalapb" %% "scalapb-json4s" % "0.3.3"
  val `google-guice` = "com.google.inject" % "guice" % "4.1.0"
  val `guava` = "com.google.guava" % "guava" % "12.0.1"
  val `typesafe-config` = "com.typesafe" % "config" % "1.3.2"
  val `enumeration` = "com.beachape" %% "enumeratum" % "1.5.13"
  val `scalaop` = "org.rogach" %% "scallop" % "3.1.4"

  val `geode` = "org.apache.geode" % "geode-core" % "1.6.0"
  val `hsqldb` = "org.hsqldb" % "hsqldb" % "1.8.0.10"  //2.4.1 gives connection error with no username/password

}

object Jackson {
  val Version = "2.9.2"
  val `jackson-core` = "com.fasterxml.jackson.core" % "jackson-core" % Version
  val `jackson-databind` = "com.fasterxml.jackson.core" % "jackson-databind" % Version
  val `jackson-module-scala` = "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version
}

object Enumeratum {
  val version = "1.5.12"
  val `enumeratum` = "com.beachape" %% "enumeratum" % version //MIT License
  val `enumeratum-play` = "com.beachape" %% "enumeratum-play" % version //MIT License
}

object Chill {
  val Version = "0.9.2"
  val `chill-akka` = "com.twitter" %% "chill-akka" % Version //Apache License 2.0
  val `chill-bijection` = "com.twitter" %% "chill-bijection" % Version //Apache License 2.0
}

object Akka {
  val Version = "2.5.10" //all akka is Apache License 2.0
  val `akka-stream` = "com.typesafe.akka" %% "akka-stream" % Version
  val `akka-remote` = "com.typesafe.akka" %% "akka-remote" % Version
  val `akka-stream-testkit` = "com.typesafe.akka" %% "akka-stream-testkit" % Version
  val `akka-actor` = "com.typesafe.akka" %% "akka-actor" % Version
  val `akka-typed` = "com.typesafe.akka" %% "akka-typed" % Version
  val `akka-typed-testkit` = "com.typesafe.akka" %% "akka-typed-testkit" % Version
  val `akka-distributed-data` = "com.typesafe.akka" %% "akka-distributed-data" % Version
  val `akka-multi-node-testkit` = "com.typesafe.akka" %% "akka-multi-node-testkit" % Version
  val `akka-cluster-tools` = "com.typesafe.akka" %% "akka-cluster-tools" % Version
  val `akka-slf4j` = "com.typesafe.akka" %% "akka-slf4j" % Version
}

object AkkaHttp {
  val Version = "10.0.10"
  val `akka-http` = "com.typesafe.akka" %% "akka-http" % Version //ApacheV2
  val `akka-http-testkit` = "com.typesafe.akka" %% "akka-http-testkit" % Version //ApacheV2
  val `akka-http2` = "com.typesafe.akka" %% "akka-http2-support" % Version
}

object Kafka {
  val `kafkaStreamsScala` = "com.lightbend" %% "kafka-streams-scala" % "0.1.0"
  val `akka-stream-kafka` = "com.typesafe.akka" %% "akka-stream-kafka" % "0.19"
  val `scalatest-embedded-kafka` = "net.manub" %% "scalatest-embedded-kafka" % "1.1.0"
}

object Sqoop {
  val `sqoop` = "org.apache.sqoop" % "sqoop" % CDHVersions.Sqoop % "provided" excludeAll(
    ExclusionRule(organization = "org.pentaho", name="pentaho-aggdesigner-algorithm"),
    ExclusionRule(organization = "eigenbase", name="eigenbase-properties"))
  val `kite-data-mapreduce` = "org.kitesdk" % "kite-data-mapreduce" % CDHVersions.`kite-data-mapreduce` % "provided"
}

object Oracle {
  val OracleVersion = "11.2.0.3"
  val `ojdbc6` = "oracle" % "ojdbc6" % OracleVersion
}

object Hadoop {
  val `hadoop-common` = "org.apache.hadoop" % "hadoop-common" % CDHVersions.Hadoop % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `hadoop-hdfs` = "org.apache.hadoop" % "hadoop-hdfs" % CDHVersions.Hadoop % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `hadoop-auth` = "org.apache.hadoop" % "hadoop-auth" % CDHVersions.Hadoop % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `hadoop-client` = "org.apache.hadoop" % "hadoop-client" % CDHVersions.Hadoop % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `hadoop-core` = "org.apache.hadoop" % "hadoop-core" % CDHVersions.Hadoop % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
}

object Spark {
  val `spark-core` = "org.apache.spark" %% "spark-core" % CDHVersions.Spark % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  val `spark-sql` = "org.apache.spark" %% "spark-sql" % CDHVersions.Spark % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  val `avro` = "com.databricks" %% "spark-avro" % "4.0.0" % "provided"
  val `spark-catalyst-test` = "org.apache.spark" %% "spark-catalyst" % CDHVersions.Spark % "test" classifier "tests"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `spark-core-test` = "org.apache.spark" %% "spark-core" % CDHVersions.Spark % "test" classifier "tests"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  val `spark-sql-test` = "org.apache.spark" %% "spark-sql" % CDHVersions.Spark % "test" classifier "tests"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))


}


object HBase {

  val `hbase-client` = "org.apache.hbase" % "hbase-client" % CDHVersions.Hbase % "provided" excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  val `hbase-server` = "org.apache.hbase" % "hbase-server" % CDHVersions.Hbase % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  val `hbase-common` = "org.apache.hbase" % "hbase-common" % CDHVersions.Hbase % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))
  val `hbase-protocol` = "org.apache.hbase" % "hbase-protocol" % CDHVersions.Hbase % "provided"  excludeAll(
    ExclusionRule(organization = "com.google.guava", "guava"))

  object TestOnly {
    val `hadoop-common-tests` = "org.apache.hadoop" % "hadoop-common" % CDHVersions.Hadoop % Test classifier "tests"  excludeAll(
      ExclusionRule(organization = "com.google.guava", "guava"))
    val `hbase-tests` = "org.apache.hbase" % "hbase" % CDHVersions.Hbase % Test classifier "tests"  excludeAll(
      ExclusionRule(organization = "com.google.guava", "guava"))
    val `hadoop-hdfs-tests` = "org.apache.hadoop" % "hadoop-hdfs" % CDHVersions.Hadoop % Test classifier "tests"  excludeAll(
      ExclusionRule(organization = "com.google.guava", "guava"))

    val `hbase-test-utils` = "org.apache.hbase" % "hbase-testing-util" % CDHVersions.Hbase % Test classifier "tests"  excludeAll(
      ExclusionRule(organization = "com.google.guava", "guava"))
  }
}

object Yaml {
  val `yaml-parser` = "net.jcazevedo" %% "moultingyaml" % "0.4.0"
}

object JSON {
  val JsonVersion = "20171018"
  val `json` = "org.json" % "json" % JsonVersion
}

object OtherJars {

  val `commons-lang3` = "org.apache.commons" % "commons-lang3" % "3.0"
  val `guava` = "com.google.guava" % "guava" % "15.0"
  val `httpcore` = "org.apache.httpcomponents" % "httpcore" % "4.2.2"
  val `spark-avro` = "com.databricks" %% "spark-avro" % "4.0.0"
  val `avro-mapred-hadoop2` = "org.apache.avro" % "avro-mapred" % "1.7.7" classifier "hadoop2"
  val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
  val `logback` = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val `logback-json-encoder` = "net.logstash.logback" % "logstash-logback-encoder" % "4.11" excludeAll ExclusionRule(
    organization = "com.fasterxml.jackson.core"
  )
}

object Excluded {
  val `slf4j-log4j12` = "org.slf4j" % "slf4j-log4j12"
}
