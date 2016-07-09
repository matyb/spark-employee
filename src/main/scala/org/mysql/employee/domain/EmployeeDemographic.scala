package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.constants.DateConstants
import org.mysql.employee.enums.Gender
import org.mysql.util.Range

case class EmployeeDemographic(employeeId: String, dob: Date, firstName: String, lastName: String, gender: Gender.Value, start: Date, end: Date = DateConstants.endOfTime) extends EmployeeId with Range {

  def this(record: Array[String], sdf: SimpleDateFormat = new SimpleDateFormat(DateConstants.ingestionDateFormat)) =
    this(record(0),
      sdf.parse(record(1).replace("'", "")),
      record(2),
      record(3),
      Gender withName record(4),
      sdf.parse(record(5).replace("'", "")))

}

object EmployeeDemographic {
  def apply(record: Array[String], sdf: SimpleDateFormat) = new EmployeeDemographic(record, sdf)
  val UNKNOWN = EmployeeDemographic("-999999", DateConstants.endOfTime, "UNKNOWN", "UNKNOWN", Gender withName "U", DateConstants.endOfTime)
}