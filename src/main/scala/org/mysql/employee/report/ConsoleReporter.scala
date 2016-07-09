package org.mysql.employee.report

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Employee

object ConsoleReporter {

  def sdf = new SimpleDateFormat(DateConstants.outputDateFormat)
  
  def report(employees: RDD[Employee]) = {
    new Reporter[String] {
      def asOf(asOfDate: Date) = {
        val filtered = employees.map { employee => employee.filter(asOfDate)}
        val active = filtered.filter { employee => employee.isEmployedAsOf(asOfDate) }                              
        val asOfString = sdf.format(asOfDate)
s"""Report as of: '$asOfString'\n==========================
--- Number employed: ${active.count()}\n"""
      }
    }
  }
  
}