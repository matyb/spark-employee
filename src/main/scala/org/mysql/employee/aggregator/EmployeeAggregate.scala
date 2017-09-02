package org.mysql.employee.aggregator

import java.util.Date
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.EmployeeAsOf
import org.mysql.employee.enums.Gender

trait EmployeeAggregate {
  
  val asOfDate : Date
  
  def activeCount() : Map[GroupBy, Long]
  
  def managersByDepartment() : Map[Department, Iterable[EmployeeAsOf]]
  
  def salaryByDepartment() : SalariesAggregate
  
}