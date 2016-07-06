package org.mysql.employee.domain

import org.mysql.employee.utils.Converter
import java.text.SimpleDateFormat

case class Department(id: String, name: String) {

  def this(record: Array[String]) = this(record(0), record(1))

}

object Department extends Converter[(Array[String], SimpleDateFormat), Department] {

  def apply(record: (Array[String], SimpleDateFormat)) = new Department(record._1)

}