package org.mysql.employee.aggregator

import java.util.Date

trait EmployeeAggregate {
  
  val asOfDate : Date
  
  def activeCount() : Long
  
  def managersByDepartment() : Map[String, List[String]]
  
  def salaryByDepartment() : SalariesAggregate
  
}