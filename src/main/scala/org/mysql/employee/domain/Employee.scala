package org.mysql.employee.domain

import java.util.Date

case class Employee(id: String,
                    departmentEmployees: List[(DepartmentEmployee, Department)], 
                    departmentManagers: List[(DepartmentManager, Department)], 
                    employeeDemographics: List[EmployeeDemographic],
                    employeeTitles: List[EmployeeTitle], 
                    employeeSalaries: List[EmployeeSalary]) {
  
  def filter(asOfDate: Date) = {
    val departmentsAsOf = departmentEmployees.filter{ t => t._1.isActiveOn(asOfDate)} 
    val departmentManagersAsOf = departmentManagers.filter{ t => t._1.isActiveOn(asOfDate)}
    val employeeDemographicsAsOf = employeeDemographics.filter{ d => d.isActiveOn(asOfDate)}
    val employeeTitlesAsOf = employeeTitles.filter{ t => t.isActiveOn(asOfDate)}  
    val employeeSalariesAsOf = employeeSalaries.filter{ t => t.isActiveOn(asOfDate)}
    EmployeeAsOf(id = this.id,
        if(departmentsAsOf.isEmpty) Department.UNKNOWN else departmentsAsOf(0)._2,
        if(departmentManagersAsOf.isEmpty) Department.UNKNOWN else departmentManagersAsOf(0)._2,
        if(employeeDemographicsAsOf.isEmpty) EmployeeDemographic.UNKNOWN else employeeDemographicsAsOf(0),
        if(employeeTitlesAsOf.isEmpty) EmployeeTitle.UNKNOWN else employeeTitlesAsOf(0),
        if(employeeSalariesAsOf.isEmpty) EmployeeSalary.UNKNOWN else employeeSalariesAsOf(0),
        asOfDate)
  } 
    
} 
