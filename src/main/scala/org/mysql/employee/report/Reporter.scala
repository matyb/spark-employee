package org.mysql.employee.report

import java.util.Date

trait Reporter[T] {
  
  def asOf(asOfDate: Date) : T
  
}