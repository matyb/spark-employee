package org.mysql.employee.aggregator

trait SalariesAggregate {
  
  def averageByDepartment() : Map[String, Long]
  
}