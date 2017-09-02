package org.mysql.employee.domain

import org.mysql.employee.constants.DateConstants
import org.scalatest.BeforeAndAfter
import org.mysql.employee.utils.DateUtils._
import org.apache.spark.rdd.RDD
import org.mysql.employee.enums.Gender
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.mysql.employee.TestData
import com.holdenkarau.spark.testing.SharedSparkContext

class EmployeeSpec extends FunSpec with Matchers with BeforeAndAfter {
  
  var data : TestData = _
  
  before {
    data = new TestData()
  }
  
  describe("when an employee is employed") {
    it("is not employed yet") {
      val asOfDate = outputFormat().parse("09/01/1993")
      data.employee.isEmployedAsOf(asOfDate) should be(false)
    }
    
    it("was just hired") {
      val asOfDate = outputFormat().parse("09/02/1993")
      data.employee.isEmployedAsOf(asOfDate) should be(true)
    }
    
    it("on last day") {
      val asOfDate = outputFormat().parse("02/03/2013")
      data.employee.isEmployedAsOf(asOfDate) should be(true)
    }
    
    it("is not employed on day after last day") {
      val asOfDate = outputFormat().parse("02/04/2013")
      data.employee.isEmployedAsOf(asOfDate) should be(false)
    }
  }
  
}