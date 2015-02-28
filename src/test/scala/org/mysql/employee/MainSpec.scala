package org.mysql.employee

import java.text.SimpleDateFormat

import org.mysql.employee.domain.Employee
import org.mysql.employee.enums.Gender
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext

class MainSpec extends FunSpec with SharedSparkContext with Matchers {

  def sdf = new SimpleDateFormat("yyyy-MM-dd")
  
  describe("Constructing Employees RDD") {

    it("reads an employee if line starts with INSERT") ({
      def records = List("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),")
      val employeesRdd = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expectedRdd = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender.withName("M"), sdf.parse("1986-06-26")))
      employeesRdd should equal (expectedRdd)
    })
    
  }

}