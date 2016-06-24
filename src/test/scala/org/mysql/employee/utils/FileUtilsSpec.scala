package org.mysql.employee.utils

import java.io.File

import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec

class FileUtilsSpec extends FlatSpec with BeforeAndAfterEach {

  val tmpFolder = if (System.getProperty("tmp.folder") != null) System.getProperty("tmp.folder") else "\\tmp\\spark-employee"

  override def beforeEach() {
    new File(tmpFolder).delete()
    assert(!new File(tmpFolder).exists())
  }

  it should "remove folders" in {
    new File(tmpFolder).createNewFile()
    assert(new File(tmpFolder).exists())
    FileUtils.rmFolder(tmpFolder)
    assert(!new File(tmpFolder).exists())
  }

}