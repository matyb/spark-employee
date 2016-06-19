package org.mysql.employee

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext

class MainSpec extends FunSpec with SharedSparkContext with Matchers {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def parse[T: ClassTag](records: Array[String], entity: Converter[Array[String], T], sc: SparkContext = sc) : RDD[T] = {
    Main.parse(sc.parallelize(records), entity)
  }
  
  def becomes[T: ClassTag](records: Array[String], entity: Converter[Array[String], T], expected: Array[T]) = {
    val results = parse(records, entity).collect().toList
    results should equal(expected)
  }

  describe("Constructing Employees RDD") {

    it("reads an employee if line starts with 'INSERT'")({
      becomes(Array("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),"), EmployeeDemographic,
        Array(EmployeeDemographic(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    })

    it("reads an employee if line starts with '('")({
      becomes(Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),"), EmployeeDemographic,
        Array(EmployeeDemographic(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    })

    it("reads an employee if line ends with ');' instead of '),'")({
      becomes(Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26');"), EmployeeDemographic,
        Array(EmployeeDemographic(10001, sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
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

  describe("Constructing Department Employee Relationship RDD") {
    
    val expectedDepartment = Department.convert(Array("d005","Some Department"))
    val expectedEmployee = EmployeeDemographic.convert(Array("10001","2015-01-01","F","L","M","2012-02-02"))
    val expectedResults = List(DepartmentEmployee.convert(expectedEmployee,expectedDepartment,"1986-06-26","9999-01-01"))
    
    def transform(line: String) = {
      val bogusEmployee1 = EmployeeDemographic.convert(Array("10000","2004-01-01","FM","LM","U","2003-02-02"))
      val bogusEmployee2 = EmployeeDemographic.convert(Array("10002","2014-01-01","FW","LW","F","2013-02-02"))
      
      val bogusDepartment1 = Department.convert(Array("d004","First Wrong Department"))
      val bogusDepartment2 = Department.convert(Array("d006","Second Wrong Department"))
      
      val employeeRdd = sc.parallelize(Seq(bogusEmployee1, expectedEmployee, bogusEmployee2))
      val departmentRdd = sc.parallelize(Seq(bogusDepartment1, expectedDepartment, bogusDepartment2))
      
      Main.parseDepartmentEmployees(employeeRdd, departmentRdd, Main.parse(sc.parallelize(Array(line)))).collect().toList
    }
    
    it("reads a department employee relationsip if line starts with 'INSERT'")({
      transform("INSERT INTO `dept_emp` VALUES (10001,'d005','1986-06-26','9999-01-01'),") should equal (expectedResults) 
    })

    it("reads a department employee relationsip if line starts with '('")({
      transform("(10001,'d005','1986-06-26','9999-01-01'),") should equal (expectedResults)
    })

    it("reads a department employee relationsip if line ends with ');' instead of '),'")({
      transform("(10001,'d005','1986-06-26','9999-01-01');") should equal (expectedResults)
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
      val employees = Main.parse(sc.parallelize(records), EmployeeDemographic).collect().toList
      records.length should equal(employees.length)
      employees(0) should equal(EmployeeDemographic(10001, sdf.parse("1953-09-02"), "Georgi", "Facello", Gender withName ("M"), sdf.parse("1986-06-26")))
      employees.last should equal(EmployeeDemographic(499999, sdf.parse("1958-05-01"), "Sachin", "Tsukuda", Gender withName "M", sdf.parse("1997-11-30")))
    }

    it("creates the same number of department records, first and last are equal to those in file") {
      val records = readRecords("load_departments.dump")
      val departments = Main.parse(sc.parallelize(records), Department).collect().toList
      records.length - 1 should equal(departments.length) // INSERT is on row by itself and is dropped
      departments(0) should equal(Department("d001", "Marketing"))
      departments.last should equal(Department("d009", "Customer Service"))
    }

    it("creates the same number of department_employee records, first and last are equal to those in file") {
      val deptRecords = readRecords("load_departments.dump")
      val departments = Main.parse(sc.parallelize(deptRecords), Department)
      val empRecords = readRecords("load_employees.dump")
      val employees = Main.parse(sc.parallelize(empRecords), EmployeeDemographic)
      
      val records = readRecords("load_dept_emp.dump")
      val departmentEmployees = Main.parseDepartmentEmployees(employees,departments,Main.parse(sc.parallelize(records))).collect().toList
      
      departmentEmployees.length should equal (records.length)
      
      val firstEmp = EmployeeDemographic.convert(Array("10001","1953-09-02","Georgi","Facello","M","1986-06-26"))
      val firstDept = Department.convert(Array("d005","Development"))
      val firstDeptEmp = List(DepartmentEmployee.convert(firstEmp, firstDept, "1986-06-26", "9999-01-01"))
      
      departmentEmployees.filter{_.employee.id == 10001} should equal (firstDeptEmp)
      
      val lastEmp = EmployeeDemographic.convert(Array("499999","1958-05-01","Sachin","Tsukuda","M","1997-11-30"))
      val lastDept = Department.convert(Array("d004","Production"))
      val lastDeptEmp = List(DepartmentEmployee.convert(lastEmp, lastDept, "1997-11-30", "9999-01-01"))
      
      departmentEmployees.filter{_.employee.id == 499999} should equal (lastDeptEmp)
    }
    
  }

}