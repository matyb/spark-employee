package org.mysql.employee.aggregator

import org.mysql.employee.utils.DateUtils._

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
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext

class RddEmployeeAggregateSpec extends FunSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  
  val outputSdf = outputFormat()
  var employees : RDD[Employee] = _
  val asOfDate = outputSdf.parse("01/01/2005");
  
  before {
    val inputSdf = ingestionFormat()
    
    val oneDepartment  = Department("d000", "Department1")
  	val twoDepartment  = Department("d001", "Department2")
  	val zeroDepartmentEmployee = DepartmentEmployee("10000", "d000", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  	val zeroDemographic = EmployeeDemographic("10000", inputSdf.parse("1953-09-02"), "Manager1", "Facello", Gender withName "M", inputSdf.parse("1983-09-02"))
  	val zeroDepartmentManager = DepartmentManager("10000", "d001", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val zeroEmployeeSalary = EmployeeSalary("10000", 95000, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val zeroEmployeeTitle = EmployeeTitle("10000","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    
    val manager1 = Employee(id = "10000",
                     departmentEmployees = List((zeroDepartmentEmployee, oneDepartment)), 
                     departmentManagers = List((zeroDepartmentManager, twoDepartment)), 
                     employeeDemographics = List(zeroDemographic),
                     employeeTitles = List(zeroEmployeeTitle), 
                     employeeSalaries = List(zeroEmployeeSalary))
    
    val oneDepartmentEmployee = DepartmentEmployee("10001", "d000", inputSdf.parse("1983-09-03"), inputSdf.parse("2013-01-03"))
  	val oneDemographic = EmployeeDemographic("10001", inputSdf.parse("1953-09-02"), "Manager2", "Facello", Gender withName "M", inputSdf.parse("1983-09-02"))
  	val oneDepartmentManager = DepartmentManager("10001", "d001", inputSdf.parse("1984-09-02"), inputSdf.parse("2006-01-03"))
    val oneEmployeeSalary = EmployeeSalary("10001", 75000, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val oneEmployeeTitle = EmployeeTitle("10001","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    
    val manager2 = Employee(id = "10001",
                     departmentEmployees = List((oneDepartmentEmployee, oneDepartment)), 
                     departmentManagers = List((oneDepartmentManager, twoDepartment)), 
                     employeeDemographics = List(oneDemographic),
                     employeeTitles = List(oneEmployeeTitle), 
                     employeeSalaries = List(oneEmployeeSalary))
                     
    val twoDepartmentEmployee = DepartmentEmployee("10002", "d001", inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
  	val twoDemographic = EmployeeDemographic("10002", inputSdf.parse("1973-09-02"), "Employee", "Facello", Gender withName "M", inputSdf.parse("1993-09-02"))
  	val twoEmployeeSalary = EmployeeSalary("10002", 34000, inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
    val twoEmployeeTitle = EmployeeTitle("10002","Not Managed",inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
    
    val employee = Employee(id = "10002",
                    departmentEmployees = List((twoDepartmentEmployee, twoDepartment)), 
                    departmentManagers = List(), 
                    employeeDemographics = List(twoDemographic),
                    employeeTitles = List(twoEmployeeTitle), 
                    employeeSalaries = List(twoEmployeeSalary))
                    
    val threeDepartmentEmployee = DepartmentEmployee("10003", "d001", inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
  	val threeDemographic = EmployeeDemographic("10003", inputSdf.parse("1998-09-02"), "ManagedEmployee", "Facello", Gender withName "M", inputSdf.parse("2003-09-02"))
  	val threeEmployeeSalary = EmployeeSalary("10003", 45000, inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
    val threeEmployeeTitle = EmployeeTitle("10003","Managed",inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
    
    val managedEmployee = Employee(id = "10003",
                    departmentEmployees = List((threeDepartmentEmployee, twoDepartment)), 
                    departmentManagers = List(), 
                    employeeDemographics = List(threeDemographic),
                    employeeTitles = List(threeEmployeeTitle), 
                    employeeSalaries = List(threeEmployeeSalary))
    
    val fourDepartmentEmployee = DepartmentEmployee("10004", "d000", inputSdf.parse("1983-09-04"), inputSdf.parse("2013-01-03"))
  	val fourDemographic = EmployeeDemographic("10004", inputSdf.parse("1953-09-02"), "Manager3", "Facello", Gender withName "M", inputSdf.parse("1983-09-02"))
  	val fourDepartmentManager = DepartmentManager("10004", "d001", inputSdf.parse("1999-09-02"), inputSdf.parse("2010-01-03"))
    val fourEmployeeSalary = EmployeeSalary("10004", 65000, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val fourEmployeeTitle = EmployeeTitle("10004","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    
    val manager3 = Employee(id = "10004",
                     departmentEmployees = List((fourDepartmentEmployee, oneDepartment)), 
                     departmentManagers = List((fourDepartmentManager, oneDepartment)), 
                     employeeDemographics = List(fourDemographic),
                     employeeTitles = List(fourEmployeeTitle), 
                     employeeSalaries = List(fourEmployeeSalary))
                    
    employees = sc.parallelize(List(manager1, manager2, manager3, employee, managedEmployee))
  }
  
  describe("supplied values") {
    
    it ("should contain as of date") {
      new RddEmployeeAggregate(employees, asOfDate).asOfDate should equal(asOfDate)
    }
    
  }
  
  describe("count of employed"){
    it ("should contain a count of active employees") {
      new RddEmployeeAggregate(employees, asOfDate).activeCount() should equal(5)
    }
    
    it ("should contain a count of active employees before anyone is employed") {
      val asOfDate = outputSdf.parse("09/01/1983")
      new RddEmployeeAggregate(employees, asOfDate).activeCount() should equal(0)
    }

    it ("should contain a count of active employees with first employee") {
      val asOfDate = outputSdf.parse("09/02/1983")
      new RddEmployeeAggregate(employees, asOfDate).activeCount() should equal(1)
    }
    
    it ("should contain a count of active employees on last day of last employee") {
      val asOfDate = outputSdf.parse("03/03/2013")
      new RddEmployeeAggregate(employees, asOfDate).activeCount() should equal(1)
    }
    
    it ("should contain a count of active employees after last day of last employee") {
      val asOfDate = outputSdf.parse("03/04/2013")
      new RddEmployeeAggregate(employees, asOfDate).activeCount() should equal(0)
    }    
  }
  
  describe("managers"){
    it ("with no departments") {
      val asOfDate = outputSdf.parse("01/01/0001")
      new RddEmployeeAggregate(employees, asOfDate).managersByDepartment() should equal(Map())
    }
    
    it ("a department with a manager") {
      new RddEmployeeAggregate(employees, asOfDate).
        managersByDepartment().get("Department1") should equal(Some(List("Facello, Manager3")))
    }
    
    it ("a department with multiple managers") {
      new RddEmployeeAggregate(employees, asOfDate).
        managersByDepartment().get("Department2") should equal(Some(List("Facello, Manager1", "Facello, Manager2")))
    }
    
  }
  
  describe("salaries") {
    
    describe("average by department"){

      it ("no employees") {
        val asOfDate = outputSdf.parse("01/01/1980")
        new RddEmployeeAggregate(employees, asOfDate).salaryByDepartment().averages() should equal(Map())
      }
      
      it ("with employees") {
        val result = new RddEmployeeAggregate(employees, asOfDate).salaryByDepartment().averages()
        result.get("Department2") should equal(Some(39500))
        result.get("Department1") should equal(Some(78333))
      }      
      
    }
    
    describe("max by department"){

      it ("no employees") {
        val asOfDate = outputSdf.parse("01/01/1980")
        new RddEmployeeAggregate(employees, asOfDate).salaryByDepartment().maximums() should equal(Map())
      }
      
      it ("with employees") {
        val result = new RddEmployeeAggregate(employees, asOfDate).salaryByDepartment().maximums()
        result.get("Department2") should equal(Some(45000))
        result.get("Department1") should equal(Some(95000))
      }      
      
    }
    
  }
  
}