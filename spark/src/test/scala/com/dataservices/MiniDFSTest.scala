package com.dataservices

import java.io.File
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.FunSuite

class MiniDFSTest extends FunSuite {
  test("sample dfs testing") {
    import org.apache.hadoop.fs.FileUtil
    import org.apache.hadoop.hdfs.MiniDFSCluster
    val conf = new Configuration()
    val baseDir = new File("./target/hdfs/").getAbsoluteFile
    FileUtil.fullyDelete(baseDir)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    val cluster = builder.build
    val hdfsURI = "hdfs://localhost:" + cluster.getNameNodePort + "/"
    cluster.waitClusterUp()

//    new MiniYARNCluster()
  }

}
