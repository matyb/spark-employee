package org.mysql.employee.utils

import org.scalatest.FlatSpec
import java.io.File
import org.scalatest.BeforeAndAfterEach

class FileUtilsSpec extends FlatSpec with BeforeAndAfterEach {

  val tmpFolder = if (System.getProperty("tmp.folder") != null) System.getProperty("tmp.folder") else "\\tmp\\spark-employee"

  override def beforeEach() {
    if (!"true".equals(System.getProperty("skip.integration.tests"))) {
      if (tmpFolder == null) throw new IllegalStateException("property 'tmp.folder' is required") 
      new File(tmpFolder).delete()
      assert(!new File(tmpFolder).exists())
    }
  }

  if (!"true".equals(System.getProperty("skip.integration.tests"))) {
    it should "remove folders" in {
      new File(tmpFolder).createNewFile()
      assert(new File(tmpFolder).exists())
      FileUtils.rmFolder(tmpFolder)
      assert(!new File(tmpFolder).exists())
    }
  }

}
