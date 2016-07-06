package org.mysql.employee.domain

import java.util.Date
import org.mysql.util.Range
import org.mysql.employee.utils.Converter
import org.mysql.employee.constants.DateConstants
import java.text.SimpleDateFormat

case class EmployeeSalary(employeeId: String, salaryDollars: Long, start: Date, end: Date) extends EmployeeId with Range[Date] {
  def this(record: Array[String], sdf: SimpleDateFormat = new SimpleDateFormat(DateConstants.ingestionDateFormat)) =
    this(record(0), record(1).toLong, sdf.parse(record(2)), sdf.parse(record(3)))
}

object EmployeeSalary extends Converter[(Array[String], SimpleDateFormat), EmployeeSalary] {
  def apply(record: (Array[String], SimpleDateFormat)) = new EmployeeSalary(record._1, record._2)
}
  
