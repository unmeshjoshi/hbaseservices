package com.hbaseservices.sqoop

import java.util

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.SqoopOptions.FileLayout
import org.apache.hadoop.mapred.JobConf

class DataImporterTest extends SqoopImportTest {

  test("should import from database to hadoop") {
    createTestData()

    new DataImporter().importData(buildSqoopOptions("employees",
      "select * from employees where name='FirstName2' AND $CONDITIONS",
      "name"))

    val targetDir = testHadoop.targetDirFor("employees")
    verifyAvroOutputFile(targetDir, List("name"), List(List("FirstName2")))
  }

  def createTestData() = {
    //TODO: Figure out if we need to set username and password explicitly.
    //    executeUpdate(String.format("CREATE USER %s PASSWORD %s ADMIN", "testuser", "testpassword"))
    val queries = List("CREATE TABLE employees (name varchar(50));",
      "INSERT INTO employees VALUES ('FirstName')",
      "INSERT INTO employees VALUES ('FirstName1')",
      "INSERT INTO employees VALUES ('FirstName2')")
    
    testDatabase.executeUpdate(queries)
  }

  def buildSqoopOptions(tableName: String, sqlQuery: String, splitByColumn: String): SqoopOptions = {
    val conf = new JobConf()
    //TODO: Figure out if we need conf to be set explicitly
    //    conf.set("mapreduce.job.queuename", "testqueue")
    //    conf.set("mapreduce.jobtracker.address", "local")
    //    conf.set("mapreduce.job.maps", "1")
    //    conf.set("fs.defaultFS", "file:///")
    //    conf.set("jobclient.completion.poll.interval", "50")
    //    conf.set("jobclient.progress.monitor.poll.interval", "50")
    //    conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS)

    val options = new SqoopOptions(conf)
    options.setConnectString(testDatabase.getJdbcConnectUrl()) //jdbc
    options.setSqlQuery(sqlQuery)
    options.setSplitByCol(splitByColumn)
    options.setNumMappers(1)
    options.setTargetDir(testHadoop.targetDirFor(tableName))
    options.setFileLayout(FileLayout.AvroDataFile)
    options.setDeleteMode(false)
    options.setAppendMode(true)
    options.setClassName("test")
    options.setCustomToolOptions(new util.HashMap[String, String]()) //need to set or else get NPE
    options
  }
}
