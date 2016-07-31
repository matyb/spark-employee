package org.mysql.employee.aggregator

trait SalariesAggregate {
  
  def averages() : Map[String, Long]
  def maximums() : Map[String, Long]
  
}