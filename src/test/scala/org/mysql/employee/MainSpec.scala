package org.mysql.employee

import java.text.SimpleDateFormat
import java.util.Enumeration

import scala.reflect._
import scala.reflect.ClassTag
import scala.reflect.Manifest
import scala.reflect.Manifest

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.domain.EmployeeSalary
import org.mysql.employee.domain.EmployeeTitle
import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Companion._
import org.mysql.employee.utils.Companion.companion
import org.scalatest.FunSpec
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.Matchers
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.Matcher

import com.holdenkarau.spark.testing.SharedSparkContext
import com.holdenkarau.spark.testing.SharedSparkContext

class MainSpec extends FunSpec with SharedSparkContext with Matchers {

  val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
  
  def parse[T: ClassTag](records: Array[String], entity: (Array[String],SimpleDateFormat) => T, sc: SparkContext = sc): RDD[T] = {
    Main.parse(sc.parallelize(records), entity)
  }

  class ShouldBecomeMatcher[T](expected: Array[T]) (implicit m : Manifest[T])  extends Matcher[Array[String]] {
    
    def apply(records: Array[String]) = {
      val comp : { def apply(record: Array[String], sdf: SimpleDateFormat): T } = companion(m)
      val results = parse(records, comp.apply(_, _)).collect().toList
      MatchResult(
        results.sameElements(expected),
        s"Records $records did not equal $results",
        s"Records $records did equal $results"
      )
    }
    
  }
  
  def become[T: ClassTag](expected: Array[T]) (implicit m : Manifest[T]) = new ShouldBecomeMatcher(expected)
  
  describe("Constructing Employees RDD") {
    
    it("reads an employee if line starts with 'INSERT'") {
      Array("INSERT INTO `employees` VALUES (10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),") should become(
        Array(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    }

    it("reads an employee if line starts with '('") {
      Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26'),") should become(
        Array(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    }

    it("reads an employee if line ends with ');' instead of '),'") {
      Array("(10001,'1953-09-02','Georgie','Facello','M','1986-06-26');") should become(
        Array(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26"))))
    }

  }

  describe("Constructing Departments RDD") {

    it("eliminates rows that it its only an insert statement") {
      Array("INSERT INTO `departments` VALUES") should become[Department](Array())
    }

    it("reads a department if line starts with '('") {
      1 should equal (1)
      Array("('d001','Marketing'),") should become(Array(Department("d001", "Marketing")))
    }

    it("reads a department if line ends with ');' instead of '),'") {
      Array("('d009','Customer Service');") should become(Array(Department("d009", "Customer Service")))
    }

  }

  describe("Constructing Department Employee Relationship RDD") {

    it("reads a department employee relationsip if line starts with 'INSERT'") {
      Array("INSERT INTO `dept_emp` VALUES (10001,'d005','1986-06-26','9999-01-01'),") should become(
        Array(DepartmentEmployee("10001", "d005", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

    it("reads a department employee relationsip if line starts with '('") {
      Array("(10001,'d005','1986-06-26','9999-01-01'),") should become(
        Array(DepartmentEmployee("10001", "d005", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

    it("reads a department employee relationsip if line ends with ');' instead of '),'") {
      Array("(10001,'d005','1986-06-26','9999-01-01');") should become(
        Array(DepartmentEmployee("10001", "d005", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

  }

  describe("Constructing Department Manager Relationship RDD") {

    it("reads a department manager relationsip if line starts with 'INSERT'") {
      Array("INSERT INTO `dept_manager` VALUES '),") should become[DepartmentManager](Array())
    }

    it("reads a department manager relationsip if line starts with '('") {
      Array("(110022,'d004','1985-01-01','1991-10-01'),") should become(
          Array(DepartmentManager("110022", "d004", sdf.parse("1985-01-01"), sdf.parse("1991-10-01"))))
    }

    it("reads a department employee relationsip if line ends with ');' instead of '),'") {
      Array("(110022,'d004','1985-01-01','1991-10-01');") should become(
          Array(DepartmentManager("110022", "d004", sdf.parse("1985-01-01"), sdf.parse("1991-10-01"))))
    }

  }

  describe("Constructing Employee Title RDD") {

    it("reads an employee title if line starts with 'INSERT'") {
      Array("INSERT INTO `titles` VALUES (10001,'Senior Engineer','1986-06-26','9999-01-01'),") should become(
        Array(EmployeeTitle("10001", "Senior Engineer", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

    it("reads an employee title if line starts with '('") {
      Array("(10001,'Senior Engineer','1986-06-26','9999-01-01'),") should become(
        Array(EmployeeTitle("10001", "Senior Engineer", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

    it("reads an employee title if line ends with ');'") {
      Array("(10001,'Senior Engineer','1986-06-26','9999-01-01');") should become(
        Array(EmployeeTitle("10001", "Senior Engineer", sdf.parse("1986-06-26"), sdf.parse("9999-01-01"))))
    }

  }
  
  describe("Constructing Employee Salary RDD") {

    it("reads an employee salary if line starts with 'INSERT'") {
      Array("INSERT INTO `salaries` VALUES (10001,60117,'1986-06-26','1987-06-26'),") should become( 
        Array(EmployeeSalary("10001", 60117, sdf.parse("1986-06-26"), sdf.parse("1987-06-26"))))
    }

    it("reads an employee salary if line starts with '('") {
      Array("(10001,60117,'1986-06-26','1987-06-26'),") should become(
        Array(EmployeeSalary("10001", 60117, sdf.parse("1986-06-26"), sdf.parse("1987-06-26"))))
    }

    it("reads an employee salary if line ends with ');'") {
      Array("(10001,60117,'1986-06-26','1987-06-26');") should become(
        Array(EmployeeSalary("10001", 60117, sdf.parse("1986-06-26"), sdf.parse("1987-06-26"))))
    }

  }

  describe("Creates employees") {
    
    it("creates an employee") {
    	val oneDepartment  = Array(Department("d000", "Department1"))
    	val oneDepartmentEmployee = Array(DepartmentEmployee("10001", "d000", sdf.parse("1953-09-02"), sdf.parse("1993-03-03")))
    	val oneDemographic = Array(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26")))
      val oneEmployeeSalary = Array(EmployeeSalary("10001", 99999999, sdf.parse("1953-09-02"), sdf.parse("1986-06-26")))
      val oneTitle = Array(EmployeeTitle("10001","Title",sdf.parse("1900-02-02"), sdf.parse("1901-02-02")))
      
      val employee = Main.join(sc.parallelize(oneDepartment), sc.parallelize(oneDepartmentEmployee), 
          sc.parallelize(Seq()), sc.parallelize(oneDemographic), sc.parallelize(oneTitle), 
          sc.parallelize(oneEmployeeSalary)).collect()
      val expectedEmployee = Employee("10001", List((oneDepartmentEmployee(0),oneDepartment(0))), List(), 
                                      List(oneDemographic(0)), List(oneTitle(0)), List(oneEmployeeSalary(0)))
      employee should equal (List(expectedEmployee))    
    }
    
    it("creates a manager") {
    	val oneDepartment  = Array(Department("d000", "Department1"))
    	val twoDepartment  = Array(Department("d001", "Department2"))
    	val oneDepartmentEmployee = Array(DepartmentEmployee("10001", "d000", sdf.parse("1953-09-02"), sdf.parse("1993-03-03")))
    	val oneDepartmentManager = Seq(DepartmentManager("10001", "d001", sdf.parse("1900-01-01"), sdf.parse("1920-02-02")))
    	val oneDemographic = Array(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgie", "Facello", Gender withName "M", sdf.parse("1986-06-26")))
      val oneEmployeeSalary = Array(EmployeeSalary("10001", 99999999, sdf.parse("1953-09-02"), sdf.parse("1986-06-26")))
      val oneTitle = Array(EmployeeTitle("10001","Title",sdf.parse("1900-02-02"), sdf.parse("1901-02-02")))
      
      val employee = Main.join(sc.parallelize(List(oneDepartment(0), twoDepartment(0))), sc.parallelize(oneDepartmentEmployee), 
          sc.parallelize(oneDepartmentManager), sc.parallelize(oneDemographic), sc.parallelize(oneTitle), 
          sc.parallelize(oneEmployeeSalary)).collect()
      val expectedEmployee = Employee("10001", 
                                      List((oneDepartmentEmployee(0),oneDepartment(0))), 
                                      List((oneDepartmentManager(0),twoDepartment(0))), List(oneDemographic(0)), 
                                      List(oneTitle(0)), List(oneEmployeeSalary(0)))
      employee should equal (Array(expectedEmployee))    
    }
    
  }
  
  def readRecords(fileName: String) = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream(s"/$fileName"))
    val text = try source.mkString finally source.close()
    text.split("\\r?\\n")
  }

  if (!"true".equals(System.getProperty("skip.integration.tests"))) {
    describe("Can construct RDDs from actual MySQL files") {

      def loadEmployees(): (Array[String], RDD[EmployeeDemographic], List[EmployeeDemographic]) = {
        val records = readRecords("load_employees.dump")
        val rdd = Main.parse(sc.parallelize(records), EmployeeDemographic(_,_))
        (records, rdd, rdd.collect().toList)
      }

      it("creates the same number of employee records, first and last are equal to those in file") {
        val (records, employeesRdd, employees) = loadEmployees()
        records.length should equal(employees.length)
        employees(0) should equal(EmployeeDemographic("10001", sdf.parse("1953-09-02"), "Georgi", "Facello", Gender withName ("M"), sdf.parse("1986-06-26")))
        employees.last should equal(EmployeeDemographic("499999", sdf.parse("1958-05-01"), "Sachin", "Tsukuda", Gender withName "M", sdf.parse("1997-11-30")))
      }

      def loadDepartments(): (Array[String], RDD[Department], List[Department]) = {
        val records = readRecords("load_departments.dump")
        val departments = Main.parse(sc.parallelize(records), Department(_,_))
        (records, departments, departments.collect().toList)
      }

      it("creates the same number of department records, first and last are equal to those in file") {
        val (records, departmentsRdd, departments) = loadDepartments()
        records.length - 1 should equal(departments.length) // INSERT is on row by itself and is dropped
        departments(0) should equal(Department("d001", "Marketing"))
        departments.last should equal(Department("d009", "Customer Service"))
      }

      def loadDepartmentEmployees(): (Array[String], RDD[DepartmentEmployee], List[DepartmentEmployee]) = {
        val records = readRecords("load_dept_emp.dump")
        val departmentEmployees = Main.parse(sc.parallelize(records), DepartmentEmployee(_,_))
        (records, departmentEmployees, departmentEmployees.collect().toList)
      }

      it("creates the same number of department_employee records, first and last are equal to those in file") {
        val (records, departmentEmployeesRdd, departmentEmployees) = loadDepartmentEmployees()

        val cachedDepartmentEmployees = departmentEmployeesRdd.cache()

        cachedDepartmentEmployees.count() should equal(records.length)

        val firstDeptEmp = List(DepartmentEmployee(Array("10001", "d005", "1986-06-26", "9999-01-01"), sdf))

        cachedDepartmentEmployees.filter { _.employeeId == "10001" }.collect() should equal(firstDeptEmp)

        val lastDeptEmp = List(DepartmentEmployee(Array("499999", "d004", "1997-11-30", "9999-01-01"), sdf))

        cachedDepartmentEmployees.filter { _.employeeId == "499999" }.collect() should equal(lastDeptEmp)
      }

      def loadDepartmentManagers(): (Array[String], RDD[DepartmentManager], List[DepartmentManager]) = {
        val records = readRecords("load_dept_manager.dump")
        val departmentManagers = Main.parse(sc.parallelize(records), DepartmentManager(_,_))
        (records, departmentManagers, departmentManagers.collect().toList)
      }

      it("creates the same number of department_manager records, first and last are equal to those in file") {
        val (records, departmentManagersRdd, departmentManagers) = loadDepartmentManagers()

        val cachedDepartmentManagers = departmentManagersRdd.cache()

        cachedDepartmentManagers.count() should equal(records.filter { row => !row.isEmpty() && !row.trim().endsWith("VALUES") }.length)

        val firstDeptMgr = List(DepartmentManager(Array("110022", "d001", "1985-01-01", "1991-10-01"), sdf))

        cachedDepartmentManagers.filter { _.employeeId == "110022" }.collect() should equal(firstDeptMgr)

        val lastDeptMgr = List(DepartmentManager(Array("111939", "d009", "1996-01-03", "9999-01-01"), sdf))

        cachedDepartmentManagers.filter { _.employeeId == "111939" }.collect() should equal(lastDeptMgr)
      }

      def loadEmployeeTitles(): (Array[String], RDD[EmployeeTitle], List[EmployeeTitle]) = {
        val records = readRecords("load_titles.dump")
        val employeeTitles = Main.parse(sc.parallelize(records), EmployeeTitle(_,_))
        (records, employeeTitles, employeeTitles.collect().toList)
      }

      it("creates the same number of employee_title records, first and last are equal to those in file") {
        val (records, employeeTitlesRdd, employeeTitles) = loadEmployeeTitles()

        val cachedEmployeeTitles = employeeTitlesRdd.cache()

        cachedEmployeeTitles.count() should equal(records.length)

        val firstEmpTitle = List(EmployeeTitle(Array("10001", "Senior Engineer", "1986-06-26", "9999-01-01"), sdf))

        cachedEmployeeTitles.filter { _.employeeId == "10001" }.collect() should equal(firstEmpTitle)

        val lastEmpTitle = List(EmployeeTitle(Array("499999", "Engineer", "1997-11-30", "9999-01-01"), sdf))

        cachedEmployeeTitles.filter { _.employeeId == "499999" }.collect() should equal(lastEmpTitle)
      }
      
    }
    
  }

}
