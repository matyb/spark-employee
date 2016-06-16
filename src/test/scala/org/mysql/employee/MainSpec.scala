package org.mysql.employee

import java.text.SimpleDateFormat

import org.mysql.employee.domain.Employee
import org.mysql.employee.enums.Gender
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department

class MainSpec extends FunSpec with SharedSparkContext with Matchers {

  def sdf = new SimpleDateFormat("yyyy-MM-dd")

  describe("Constructing Employees RDD") {

    it("reads an employee if line starts with 'INSERT'")({
      def records = List("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26")))
      employees should equal(expected)
    })

    it("reads an employee if line starts with '('")({
      def records = List("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26")))
      employees should equal(expected)
    })

    it("reads an employee if line ends with ');' instead of '),'")({
      def records = List("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26');")
      val employees = Main.parseEmployees(sc.parallelize(records)).collect().toList
      val expected = List(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26")))
      employees should equal(expected)
    })

  }

  describe("Constructing Departments RDD") {

    it("eliminates rows that it its only an insert statement")({
      def records = List("INSERT INTO `departments` VALUES")
      val departments = Main.parseDepartments(sc.parallelize(records)).collect().toList
      val expected = List()
      departments should equal(expected)
    })
    
    it("reads a department if line starts with '('")({
      def records = List("('d001','Marketing'),")
      val departments = Main.parseDepartments(sc.parallelize(records)).collect().toList
      val expected = List(Department("d001", "Marketing"))
      departments should equal(expected)
    })
    
    it("reads a department if line ends with ');' instead of '),'")({
      def records = List("('d009','Customer Service');")
      val departments = Main.parseDepartments(sc.parallelize(records)).collect().toList
      val expected = List(Department("d009", "Customer Service"))
      departments should equal(expected)
    })

  }
  
  describe("Can construct RDDs from actual MySQL files") {
     
    it("creates the same number of employee records, first and last are equal to those in file") {
      val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/load_employees.dump"))
      val text = try source.mkString finally source.close()
      val records = text.split("\\r?\\n")
      val employeesRdd = Main.parseEmployees(sc.parallelize(records))
      val employees = employeesRdd.collect()
      records.length should equal(employeesRdd.count())
      employees(0) should equal(Employee(10001, sdf.parse("1953-09-02"), "Georgi", "Facello", Gender withName ("M"), sdf.parse("1986-06-26")))
      employees.last should equal(Employee(499999, sdf.parse("1958-05-01"), "Sachin", "Tsukuda", Gender withName "M", sdf.parse("1997-11-30")))
    }
    
    it("creates the same number of department records, first and last are equal to those in file") {
      val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/load_departments.dump"))
      val text = try source.mkString finally source.close()
      val records = text.split("\\r?\\n")
      val departmentsRdd = Main.parseDepartments(sc.parallelize(records))
      val departments = departmentsRdd.collect()
      records.length - 1 should equal(departmentsRdd.count()) // INSERT is on row by itself
      departments(0) should equal(Department("d001", "Marketing"))
      departments.last should equal(Department("d009", "Customer Service"))
    }

  }

}