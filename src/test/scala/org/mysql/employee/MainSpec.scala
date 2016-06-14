package org.mysql.employee

import java.text.SimpleDateFormat

import org.mysql.employee.domain.Employee
import org.mysql.employee.enums.Gender
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.BeforeAndAfterAll

class MainSpec extends FunSpec with SharedSparkContext with Matchers with BeforeAndAfterAll {

  def sdf = new SimpleDateFormat("yyyy-MM-dd")
  
  describe("Constructing Employees RDD") {

    it("reads an employee if line starts with 'INSERT'") ({
      def records = List("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender.withName("M"), sdf.parse("1986-06-26")))
      employees should equal (expected)
    })
    
    it("reads an employee if line starts with '('") ({
      def records = List("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender.withName("M"), sdf.parse("1986-06-26")))
      employees should equal (expected)
    })
    
    it("reads an employee if line ends with ');' instead of '),'") ({
      def records = List("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26');")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender.withName("M"), sdf.parse("1986-06-26")))
      employees should equal (expected)
    })
    
  }
  
  describe("Can construct Employees RDD from actual MySQL file") {
    
    it("makes sense of each record in file"){
      val clazz = getClass
      val source = scala.io.Source.fromInputStream(clazz.getResourceAsStream("/load_employees.dump"))
      val records = try source.mkString.split("\r\n") finally source.close()
      val employeesRdd = Main.parseEmployees(sc.parallelize(records))
      
      records.length should equal (employeesRdd.count())
    }
    
  }

}