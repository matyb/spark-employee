package org.mysql.employee.domain

import org.mysql.employee.utils.Converter

case class Department(id: String, name: String)

object Department extends Converter[Array[String], Department] {
  
  def convert(array: Array[String]): Department = {
    Department(array(0), array(1))
  }
  
} 