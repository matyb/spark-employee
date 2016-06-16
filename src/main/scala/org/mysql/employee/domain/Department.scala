package org.mysql.employee.domain

case class Department(id: String, name: String)

object Department {
  
  def fromArray(array: Array[String]): Department = {
    Department(array(0), array(1))
  }
  
} 