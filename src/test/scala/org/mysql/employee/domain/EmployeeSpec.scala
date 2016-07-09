package org.mysql.employee.domain

import org.mysql.employee.constants.DateConstants
import org.scalatest.BeforeAndAfter
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.mysql.employee.enums.Gender
import org.scalatest.FunSpec
import org.scalatest.Matchers

class EmployeeSpec extends FunSpec with Matchers with BeforeAndAfter {
  
  val outputSdf = new SimpleDateFormat(DateConstants.outputDateFormat)
  var employee : Employee = _
  var manager : Employee = _
  var dateString = "01/01/2005"
  var asOfDate = outputSdf.parse(dateString)
  
  before {
    val inputSdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    
    val oneDepartment  = Department("d000", "Department1")
  	val twoDepartment  = Department("d001", "Department2")
  	val oneDepartmentEmployee = DepartmentEmployee("10001", "d000", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  	val oneDemographic = EmployeeDemographic("10001", inputSdf.parse("1953-09-02"), "Manager", "Facello", Gender withName "M", inputSdf.parse("1983-09-02"))
  	val oneDepartmentManager = DepartmentManager("10001", "d001", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val oneEmployeeSalary = EmployeeSalary("10001", 99999999, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    val oneEmployeeTitle = EmployeeTitle("10001","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
    
    manager = Employee(id = "10001",
                    departmentEmployees = List((oneDepartmentEmployee, oneDepartment)), 
                    departmentManagers = List((oneDepartmentManager, twoDepartment)), 
                    employeeDemographics = List(oneDemographic),
                    employeeTitles = List(oneEmployeeTitle), 
                    employeeSalaries = List(oneEmployeeSalary))
    
  	val twoDepartmentEmployee = DepartmentEmployee("10002", "d000", inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
  	val twoDemographic = EmployeeDemographic("10002", inputSdf.parse("1973-09-02"), "Employee", "Facello", Gender withName "M", inputSdf.parse("1993-09-02"))
  	val twoDepartmentManager = DepartmentManager("10002", "d001", inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
    val twoEmployeeSalary = EmployeeSalary("10002", 99999999, inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
    val twoEmployeeTitle = EmployeeTitle("10002","Not Managed",inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
    
    employee = Employee(id = "10002",
                    departmentEmployees = List((twoDepartmentEmployee, twoDepartment)), 
                    departmentManagers = List(), 
                    employeeDemographics = List(twoDemographic),
                    employeeTitles = List(twoEmployeeTitle), 
                    employeeSalaries = List(twoEmployeeSalary))
  }
  
  describe("when an employee is employed") {
    it("is not employed yet") {
      val asOfDate = outputSdf.parse("09/01/1993")
      employee.filter(asOfDate).isEmployedAsOf(asOfDate) should be(false)
    }
    
    it("was just hired") {
      val asOfDate = outputSdf.parse("09/02/1993")
      employee.filter(asOfDate).isEmployedAsOf(asOfDate) should be(true)
    }
    
    it("on last day") {
      val asOfDate = outputSdf.parse("02/03/2013")
      employee.filter(asOfDate).isEmployedAsOf(asOfDate) should be(true)
    }
    
    it("is not employed on day after last day") {
      val asOfDate = outputSdf.parse("02/04/2013")
      employee.filter(asOfDate).isEmployedAsOf(asOfDate) should be(false)
    }
  }
  
}