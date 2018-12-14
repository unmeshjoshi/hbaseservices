package com.hbaseservices.sqoop

import java.io.File
import java.sql.{Connection, DriverManager, SQLException}
import java.util

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.FileLayout
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.mapred.JobConf
import org.hsqldb.Server
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SqoopImportTestBase extends FunSuite with BeforeAndAfterAll {
  val TARGET_DIR = "/tmp/hadoop"
  private val IN_MEM = "mem:"
  private val tmpDir: String = System.getProperty("test.build.data", "/tmp/")
  private val EMPLOYEE_TABLE_NAME = "EMPLOYEES"
  private val DB_URL = "jdbc:hsqldb:" + getServerHost + DATABASE_NAME
  private val DATABASE_NAME = System.getProperty("hsql.database.name", "db1")
  private val DRIVER_CLASS = "org.hsqldb.jdbcDriver"
  var dbLocation: String = tmpDir + "/sqoop/testdb.file"

  def getServerHost: String = {
    var host = System.getProperty("hsql.server.host", IN_MEM)
    if (!host.endsWith("/")) host += "/"
    host
  }

  val server = new Server

  override def beforeAll(): Unit = {
    startDatabase()
    createTestData()
  }

  private def startDatabase ()= {
    server.setDatabaseName(0, "test")
    server.putPropertiesFromString("database.0=" + dbLocation + ";no_system_exit=true")
    server.start
  }

  override def afterAll(): Unit = {
    server.stop
    FileUtils.deleteDirectory(new File(TARGET_DIR))
  }

  def createTestData() = {
    //TODO: Figure out if we need to set username and password explicitly.
    //    executeUpdate(String.format("CREATE USER %s PASSWORD %s ADMIN", "testuser", "testpassword"))
    executeUpdate("CREATE TABLE " + EMPLOYEE_TABLE_NAME + "(name VARCHAR(50));")
    executeUpdate("INSERT INTO " + EMPLOYEE_TABLE_NAME + " VALUES ('FirstName')")
    executeUpdate("INSERT INTO " + EMPLOYEE_TABLE_NAME + " VALUES ('FirstName1')")
    executeUpdate("INSERT INTO " + EMPLOYEE_TABLE_NAME + " VALUES ('FirstName2')")
  }

  private def executeUpdate(query: String) = {
    val connection = getConnection("", "")
    val statement = connection.createStatement
    try {
      try
        statement.executeUpdate(query)
        connection.commit()
    } finally {
      if (connection != null) connection.close()
      if (statement != null) statement.close()
    }

  }

  @throws[SQLException]
  def getConnection(user: String, password: String): Connection = {
    try
      Class.forName(DRIVER_CLASS)
    catch {
      case cnfe: ClassNotFoundException ⇒
        println("Could not get connection; driver class not found: " + DRIVER_CLASS)
        return null
    }
    val connection = DriverManager.getConnection(DB_URL)
    connection.setAutoCommit(false)
    connection
  }

  def buildSqoopOptions() = {
    val conf = new JobConf() //TODO: Figure out if we need conf to be set explicitly
    //    conf.set("mapreduce.job.queuename", "testqueue")
    //    conf.set("mapreduce.jobtracker.address", "local")
    //    conf.set("mapreduce.job.maps", "1")
    //    conf.set("fs.defaultFS", "file:///")
    //    conf.set("jobclient.completion.poll.interval", "50")
    //    conf.set("jobclient.progress.monitor.poll.interval", "50")
    //    conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS)

    val options = new SqoopOptions(conf)
    options.setConnectString(DB_URL) //jdbc
    options.setSqlQuery("select * from " + EMPLOYEE_TABLE_NAME)
    options.setSplitByCol("name")
    options.setNumMappers(1)
    options.setTargetDir(TARGET_DIR)
    options.setFileLayout(FileLayout.AvroDataFile)
    options.setDeleteMode(false)
    options.setAppendMode(true)
    options.setClassName("test")
    options.setTableName(EMPLOYEE_TABLE_NAME)
    options.setCustomToolOptions(new util.HashMap[String, String]()) //need to set or else get NPE
    options
  }
}
