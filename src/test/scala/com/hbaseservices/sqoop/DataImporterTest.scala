package com.hbaseservices.sqoop

import java.io.{File, FilenameFilter}

class DataImporterTest extends SqoopImportTestBase {

  def verifyAvroOutputFile(targetDir: String): Unit = {
    val files = new File(targetDir).listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".avro")
    })
    assert(files.size == 1)
  }

  test("should import from database to hadoop") {
    new DataImporter().importData(buildSqoopOptions())
    verifyAvroOutputFile(TARGET_DIR)
  }
}
