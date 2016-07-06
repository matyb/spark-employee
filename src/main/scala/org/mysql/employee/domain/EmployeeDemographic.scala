package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter
import org.mysql.employee.constants.DateConstants

case class EmployeeDemographic(employeeId: String, dob: Date, firstName: String, lastName: String, gender: Gender.Value, hireDate: Date) extends EmployeeId {

  def this(record: Array[String], sdf: SimpleDateFormat = new SimpleDateFormat(DateConstants.ingestionDateFormat)) =
    this(record(0),
      sdf.parse(record(1).replace("'", "")),
      record(2),
      record(3),
      Gender withName record(4),
      sdf.parse(record(5).replace("'", "")))

}

object EmployeeDemographic extends Converter[(Array[String], SimpleDateFormat), EmployeeDemographic] {
  def apply(record: (Array[String], SimpleDateFormat)) = new EmployeeDemographic(record._1, record._2)
}