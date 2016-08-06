package org.mysql.employee

import org.mysql.employee.domain.Department
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.EmployeeTitle
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.EmployeeSalary
import org.mysql.employee.domain.Employee
import org.mysql.employee.utils.DateUtils._
import org.mysql.employee.enums.Gender
import org.apache.spark.SparkContext

class TestData() {
  val oneDepartment  = Department("d000", "Department1")
  val twoDepartment  = Department("d001", "Department2")
  
  val inputSdf = ingestionFormat()
  
  val asOfDate = inputSdf.parse("2005-01-01")
  
	val zeroDepartmentEmployee = DepartmentEmployee("10000", "d000", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
	val zeroDemographic = EmployeeDemographic("10000", inputSdf.parse("1953-09-02"), "Manager1", "Facello", Gender withName "M", inputSdf.parse("1983-09-02"))
	val zeroDepartmentManager = DepartmentManager("10000", "d001", inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  val zeroEmployeeSalary = EmployeeSalary("10000", 95000, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  val zeroEmployeeTitle = EmployeeTitle("10000","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  
  val manager1 = Employee(id = "10000",
                          departmentEmployees = List((zeroDepartmentEmployee, oneDepartment)),
                          departmentsManaged = List((zeroDepartmentManager, twoDepartment)),
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
                          departmentsManaged = List((oneDepartmentManager, twoDepartment)),
                          employeeDemographics = List(oneDemographic),
                          employeeTitles = List(oneEmployeeTitle), 
                          employeeSalaries = List(oneEmployeeSalary))
                   
  val twoDepartmentEmployee = DepartmentEmployee("10002", "d001", inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
	val twoDemographic = EmployeeDemographic("10002", inputSdf.parse("1973-09-02"), "Employee", "Facello", Gender withName "M", inputSdf.parse("1993-09-02"))
	val twoEmployeeSalary = EmployeeSalary("10002", 34000, inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
  val twoEmployeeTitle = EmployeeTitle("10002","Not Managed",inputSdf.parse("1993-09-02"), inputSdf.parse("2013-02-03"))
  
  val employee = Employee(id = "10002",
                  departmentEmployees = List((twoDepartmentEmployee, twoDepartment)),
                  departmentsManaged = List(),
                  employeeDemographics = List(twoDemographic),
                  employeeTitles = List(twoEmployeeTitle), 
                  employeeSalaries = List(twoEmployeeSalary))
                  
  val threeDepartmentEmployee = DepartmentEmployee("10003", "d001", inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
	val threeDemographic = EmployeeDemographic("10003", inputSdf.parse("1998-09-02"), "ManagedEmployee", "Facello", Gender withName "F", inputSdf.parse("2003-09-02"))
	val threeEmployeeSalary = EmployeeSalary("10003", 45000, inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
  val threeEmployeeTitle = EmployeeTitle("10003","Managed",inputSdf.parse("2003-09-02"), inputSdf.parse("2013-03-03"))
  
  val managedEmployee = Employee(id = "10003",
                  departmentEmployees = List((threeDepartmentEmployee, twoDepartment)), 
                  departmentsManaged = List(),
                  employeeDemographics = List(threeDemographic),
                  employeeTitles = List(threeEmployeeTitle), 
                  employeeSalaries = List(threeEmployeeSalary))
  
  val fourDepartmentEmployee = DepartmentEmployee("10004", "d000", inputSdf.parse("1983-09-04"), inputSdf.parse("2013-01-03"))
	val fourDemographic = EmployeeDemographic("10004", inputSdf.parse("1953-09-02"), "Manager3", "Facello", Gender withName "F", inputSdf.parse("1983-09-02"))
	val fourDepartmentManager = DepartmentManager("10004", "d001", inputSdf.parse("1999-09-02"), inputSdf.parse("2010-01-03"))
  val fourEmployeeSalary = EmployeeSalary("10004", 65000, inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  val fourEmployeeTitle = EmployeeTitle("10004","Manager",inputSdf.parse("1983-09-02"), inputSdf.parse("2013-01-03"))
  
  val manager3 = Employee(id = "10004",
                          departmentEmployees = List((fourDepartmentEmployee, oneDepartment)), 
                          departmentsManaged = List((fourDepartmentManager, oneDepartment)),
                          employeeDemographics = List(fourDemographic),
                          employeeTitles = List(fourEmployeeTitle), 
                          employeeSalaries = List(fourEmployeeSalary))
                  
  val employees = List[Employee](manager1, manager2, manager3, employee, managedEmployee)
  
}