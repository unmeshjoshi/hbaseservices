package com.hbaseservices.sqoop

import java.io.{File, FilenameFilter}
import java.sql.{Connection, DriverManager, SQLException}

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hsqldb.Server
import org.scalatest.{BeforeAndAfterAll, FunSuite}

abstract class SqoopImportTest extends FunSuite with BeforeAndAfterAll {
  private val tempDir = "/tmp"
  val testHadoop = new TestHadoop(tempDir)
  val testDatabase = new TestDatabase(tempDir)

  override def beforeAll(): Unit = {
    testDatabase.start()
  }

  override def afterAll(): Unit = {
    testDatabase.stop()
    testHadoop.deleteAll()
  }

  def verifyAvroOutputFile(targetDir: String, names: List[String], valueList: List[List[String]]): Unit = {
    val files = new File(targetDir).listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".avro")
    })
    assert(files.size == 1)
    val reader = read(new Path(files(0).getAbsolutePath), new Configuration())
    valueList.foreach(values ⇒ {
      val record = reader.next()
      validateRecord(record, names, values)
    }
    )
  }

  def validateRecord(record: GenericRecord, names: List[String], values: List[String]) = {
    val tuples = names.zip(values)
    tuples.foreach(tuple ⇒ {
      assert(record.get(tuple._1.toUpperCase()).toString == tuple._2)
     })

  }

  def read(filename: Path, conf: Configuration): DataFileReader[GenericRecord] = {
    conf.set("fs.defaultfs.name", "fs.defaultfs.name")
    val fsInput = new FsInput(filename, conf)
    val datumReader = new GenericDatumReader[GenericRecord]
    new DataFileReader[GenericRecord](fsInput, datumReader)
  }

  class TestHadoop(val baseDir: String) {
    val baseHadoopDir = s"${baseDir}/hadoop"

    def targetDirFor(tableName: String) = {
      s"${baseHadoopDir}/${tableName}"
    }

    def deleteAll() = {
      FileUtils.deleteDirectory(new File(baseHadoopDir))
    }
  }

  class TestDatabase(val baseDir: String) {
    private val DB_URL = "jdbc:hsqldb:mem:db1"
    private val DRIVER_CLASS = "org.hsqldb.jdbcDriver"
    private val dbLocation: String = s"${baseDir}/sqoop/testdb.file"
    private val server = new Server

    def start() = {
      server.setDatabaseName(0, "test")
      server.putPropertiesFromString("database.0=" + dbLocation + ";no_system_exit=true")
      server.start
    }

    def executeUpdate(queries: List[String]) = {
      val connection = getConnection("", "")
      val statement = connection.createStatement
      try {
        queries.foreach(query ⇒ {
          statement.executeUpdate(query)
        })
        connection.commit()
      } finally {
        if (connection != null) connection.close()
        if (statement != null) statement.close()
      }
    }

    @throws[SQLException]
    private def getConnection(user: String, password: String): Connection = {
      try
        Class.forName(DRIVER_CLASS)
      catch {
        case cnfe: ClassNotFoundException ⇒
          println("Could not get connection; driver class not found: " + DRIVER_CLASS)
          return null
      }
      val connection = DriverManager.getConnection(getJdbcConnectUrl())
      connection.setAutoCommit(false)
      connection
    }

    def getJdbcConnectUrl() = DB_URL

    def stop() = server.stop()
  }

}
