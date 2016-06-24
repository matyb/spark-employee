package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.constants.DateConstants
import org.mysql.employee.utils.Converter
import org.mysql.util.Range

case class EmployeeTitle(employeeId: String, title: String, start: Date, end: Date) extends EmployeeId with Range[Date]

object EmployeeTitle extends Converter[Array[String], EmployeeTitle] {
  
  def convert(array: Array[String]): EmployeeTitle = {
    val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    EmployeeTitle(array(0), array(1), sdf.parse(array(2)), sdf.parse(array(3)))
  }
  
} 