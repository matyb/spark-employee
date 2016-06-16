package org.mysql.employee

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import org.mysql.employee.domain.Department
import org.mysql.employee.domain.Employee
import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext

class MainSpec extends FunSpec with SharedSparkContext with Matchers {

  def sdf = new SimpleDateFormat("yyyy-MM-dd")

  def becomes[T: ClassTag](records: Array[String], entity: Converter[Array[String], T], expected: Array[T]) = {
    val results = Main.parse(sc.parallelize(records), entity).collect().toList
    results should equal(expected)
  }

  describe("Constructing Employees RDD") {

    it("reads an employee if line starts with 'INSERT'")({
      becomes(Array("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),"), Employee,
        Array(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    })

    it("reads an employee if line starts with '('")({
      becomes(Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),"), Employee,
        Array(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    })

    it("reads an employee if line ends with ');' instead of '),'")({
      becomes(Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26');"), Employee,
        Array(Employee(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    })

  }

  describe("Constructing Departments RDD") {

    it("eliminates rows that it its only an insert statement")({
      becomes(Array("INSERT INTO `departments` VALUES"), Department, Array())
    })

    it("reads a department if line starts with '('")({
      becomes(Array("('d001','Marketing'),"), Department, Array(Department("d001", "Marketing")))
    })

    it("reads a department if line ends with ');' instead of '),'")({
      becomes(Array("('d009','Customer Service');"), Department, Array(Department("d009", "Customer Service")))
    })

  }

  def readRecords(fileName: String) = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream(s"/$fileName"))
    val text = try source.mkString finally source.close()
    text.split("\\r?\\n")
  }

  describe("Can construct RDDs from actual MySQL files") {

    it("creates the same number of employee records, first and last are equal to those in file") {
      val records = readRecords("load_employees.dump")
      val employees = Main.parse(sc.parallelize(records), Employee).collect().toList
      records.length should equal(employees.length)
      employees(0) should equal(Employee(10001, sdf.parse("1953-09-02"), "Georgi", "Facello", Gender withName ("M"), sdf.parse("1986-06-26")))
      employees.last should equal(Employee(499999, sdf.parse("1958-05-01"), "Sachin", "Tsukuda", Gender withName "M", sdf.parse("1997-11-30")))
    }

    it("creates the same number of department records, first and last are equal to those in file") {
      val records = readRecords("load_departments.dump")
      val departments = Main.parse(sc.parallelize(records), Department).collect().toList
      records.length - 1 should equal(departments.length) // INSERT is on row by itself and is dropped
      departments(0) should equal(Department("d001", "Marketing"))
      departments.last should equal(Department("d009", "Customer Service"))
    }

  }

}