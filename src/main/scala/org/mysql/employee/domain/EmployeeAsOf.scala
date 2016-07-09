package org.mysql.employee.domain

import java.util.Date

case class EmployeeAsOf (id: String,
                         department: Department, 
                         managedDepartment: Department, 
                         employeeDemographic: EmployeeDemographic,
                         employeeTitle: EmployeeTitle, 
                         employeeSalary: EmployeeSalary,
                         asOfDate: Date) {
  
  def isEmployedAsOf(date: Date) = employeeDemographic != EmployeeDemographic.UNKNOWN && department != Department.UNKNOWN
  
}