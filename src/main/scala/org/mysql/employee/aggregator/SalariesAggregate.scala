package org.mysql.employee.aggregator

import org.mysql.employee.domain.Department
import scala.collection.Map
import org.mysql.employee.enums.Gender

trait SalariesAggregate {
  
  def averages() : Map[GroupBy, Long]
  def maximums() : Map[GroupBy, Long]
  
}