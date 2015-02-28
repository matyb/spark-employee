

package org.mysql.employee.utils

import scalax.file.Path
import scala.util.Try

object FileUtils {
  
  def rmFolder(outputPath: String) = {
    if(outputPath.startsWith("hdfs://")) rmHdfsFile(outputPath) else rmLocalFile(outputPath)
  }

  def rmHdfsFile(outputPath: String) = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(outputPath), true) } catch { case _ : Throwable => { } }
  }

  def rmLocalFile(outputPath: String) = {
    val path = Path (outputPath.replace("\\", "/"))
    Try(path.deleteRecursively(continueOnFailure = false))
  }
  
}