package org.mysql.employee.domain

import java.util.Date

case class Employee(id: String,
                    departmentEmployees: List[(DepartmentEmployee, Department)],
                    departmentsManaged: List[(DepartmentManager, Department)],
                    employeeDemographics: List[EmployeeDemographic],
                    employeeTitles: List[EmployeeTitle], 
                    employeeSalaries: List[EmployeeSalary]) {
  
  def getDepartment(asOfDate: Date) = {
    departmentEmployees.filter{ t => t._1.isActiveOn(asOfDate)}.headOption
  }
  
  def getEmployeeDemographic(asOfDate: Date) = {
    employeeDemographics.filter{ t => t.isActiveOn(asOfDate)}.headOption
  }
  
  def getEmployeeTitle(asOfDate: Date) = {
    employeeTitles.filter{ t => t.isActiveOn(asOfDate)}.headOption
  }
  
  def getEmployeeSalary(asOfDate: Date) = {
    employeeSalaries.filter{ t => t.isActiveOn(asOfDate)}.headOption
  }
  
  def getDepartmentManaging(asOfDate: Date) = {
    departmentsManaged.filter{ t => t._1.isActiveOn(asOfDate)}.take(1).headOption
  }
  
  def isEmployedAsOf(asOfDate: Date) = {
    getEmployeeDemographic(asOfDate) != None && getDepartment(asOfDate) != None 
  }
    
  def asOf(asOfDate: Date) = {
    EmployeeAsOf(id, getDepartment(asOfDate).get, getDepartmentManaging(asOfDate), getEmployeeDemographic(asOfDate).get, getEmployeeTitle(asOfDate).get, getEmployeeSalary(asOfDate).get, asOfDate)
  }
  
} 
