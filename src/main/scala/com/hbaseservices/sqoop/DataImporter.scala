package com.hbaseservices.sqoop

import com.cloudera.sqoop.SqoopOptions
import com.cloudera.sqoop.tool.ImportTool

class DataImporter {

  def importData(options:SqoopOptions) = {
    val importTool = new ImportTool().run(options)
  }
}
