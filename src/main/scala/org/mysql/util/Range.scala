package org.mysql.util

import java.util.Date

trait Range {
  
  def start: Date
  def end: Date
  
  def isActiveOn(asOfDate: Date) = !start.after(asOfDate) && !end.before(asOfDate)
  
}