package org.mysql.employee.report

import java.util.Date
import org.mysql.employee.aggregator.EmployeeAggregate

trait Reporter[T] {
  
  def report(aggregate: EmployeeAggregate) : T
  
}