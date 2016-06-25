package org.mysql.employee.domain

case class Employee(id: String,
          departmentEmployees: List[(DepartmentEmployee, Department)], 
          departmentManagers: List[(Department, DepartmentManager)], 
          employeeDemographics: List[EmployeeDemographic],
          employeeTitles: List[EmployeeTitle], 
          employeeSalaries: List[EmployeeSalary]) 
