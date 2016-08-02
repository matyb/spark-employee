package org.mysql.employee.domain

import java.util.Date

case class Employee(id: String,
                    departmentEmployees: List[(DepartmentEmployee, Department)], 
                    departmentManagers: List[(DepartmentManager, Department)], 
                    employeeDemographics: List[EmployeeDemographic],
                    employeeTitles: List[EmployeeTitle], 
                    employeeSalaries: List[EmployeeSalary]) {
  
  def getDepartment(asOfDate: Date) = {
    departmentEmployees.filter{ t => t._1.isActiveOn(asOfDate)}.map(_._2).foldLeft(Department.UNKNOWN){(x, y) => y}
  }
  
  def getDepartmentManaging(asOfDate: Date) = {
    departmentManagers.filter{ t => t._1.isActiveOn(asOfDate)}.map(_._2).foldLeft(Department.UNKNOWN){(x, y) => y}
  }
  
  def getEmployeeDemographic(asOfDate: Date) = {
    employeeDemographics.filter{ t => t.isActiveOn(asOfDate)}.foldLeft(EmployeeDemographic.UNKNOWN){(x, y) => y}
  }
  
  def getEmployeeTitle(asOfDate: Date) = {
    employeeTitles.filter{ t => t.isActiveOn(asOfDate)}.foldLeft(EmployeeTitle.UNKNOWN){(x, y) => y}
  }
  
  def getEmployeeSalary(asOfDate: Date) = {
    employeeSalaries.filter{ t => t.isActiveOn(asOfDate)}.foldLeft(EmployeeSalary.UNKNOWN){(x, y) => y}
  }
  
  def filter(asOfDate: Date) = {
    EmployeeAsOf(id = this.id,
        getDepartment(asOfDate),
        getDepartmentManaging(asOfDate),
        getEmployeeDemographic(asOfDate),
        getEmployeeTitle(asOfDate),
        getEmployeeSalary(asOfDate),
        asOfDate)
  } 
    
} 
