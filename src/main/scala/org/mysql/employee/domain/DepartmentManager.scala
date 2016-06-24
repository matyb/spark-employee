package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.constants.DateConstants
import org.mysql.employee.utils.Converter
import org.mysql.util.Range

case class DepartmentManager(employeeId: String, managedDepartmentId: String, start: Date, end: Date) extends EmployeeId with Range[Date]

object DepartmentManager extends Converter[Array[String],DepartmentManager] {
  
  def convert(args: Array[String]): DepartmentManager = {
    val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    DepartmentManager(args(0), args(1), sdf.parse(args(2)), sdf.parse(args(3)))
  }
  
} 