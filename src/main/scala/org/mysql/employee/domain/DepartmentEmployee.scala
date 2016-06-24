package org.mysql.employee.domain

import java.text.SimpleDateFormat

import java.util.Date

import org.mysql.util.Range
import org.mysql.employee.utils.Converter
import org.mysql.employee.constants.DateConstants
import org.mysql.util.Range

case class DepartmentEmployee(employeeId: String, departmentId: String, start: Date, end: Date) extends EmployeeId with Range[Date]

object DepartmentEmployee extends Converter[Array[String],DepartmentEmployee] {
  
  def convert(args: Array[String]): DepartmentEmployee = {
    val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    DepartmentEmployee(args(0), args(1), sdf.parse(args(2)), sdf.parse(args(3)))
  }
  
} 