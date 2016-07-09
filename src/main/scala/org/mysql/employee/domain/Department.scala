package org.mysql.employee.domain

import java.text.SimpleDateFormat

case class Department(id: String, name: String) {

  def this(record: Array[String]) = this(record(0), record(1))

}

object Department {

  def apply(record: Array[String], sdf: SimpleDateFormat) = new Department(record)

  val UNKNOWN = Department("xxxx", "Unknown")
  
}