package org.mysql.employee.domain

import java.util.Date

case class EmployeeAsOf(id: String,
                       departmentEmployee: (DepartmentEmployee, Department),
                       departmentManaged: Option[(DepartmentManager, Department)],
                       employeeDemographic: EmployeeDemographic,
                       employeeTitle: EmployeeTitle, 
                       employeeSalary: EmployeeSalary,
                       asOfDate: Date) {
    
}