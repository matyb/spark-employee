package org.mysql.employee.domain

import java.util.Date
import org.mysql.util.Range
import org.mysql.employee.utils.Converter
import org.mysql.employee.constants.DateConstants
import java.text.SimpleDateFormat

case class EmployeeSalary(employeeId: String, salaryDollars: Long, start: Date, end: Date) extends EmployeeId with Range[Date]

object EmployeeSalary extends Converter[Array[String], EmployeeSalary] {
  def convert(from: Array[String]): EmployeeSalary = {
    val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    EmployeeSalary(from(0), from(1).toLong, sdf.parse(from(2)), sdf.parse(from(3)))
  }
}