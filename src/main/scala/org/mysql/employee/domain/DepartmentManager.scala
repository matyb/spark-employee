package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.constants.DateConstants
import org.mysql.util.Range

case class DepartmentManager(employeeId: String, managedDepartmentId: String, start: Date, end: Date) extends EmployeeId with Range {

  def this(record: Array[String], sdf: SimpleDateFormat = new SimpleDateFormat(DateConstants.ingestionDateFormat)) =
    this(record(0), record(1), sdf.parse(record(2)), sdf.parse(record(3)))

}

object DepartmentManager {
  def apply(record: Array[String], sdf: SimpleDateFormat) = new DepartmentManager(record, sdf)
}