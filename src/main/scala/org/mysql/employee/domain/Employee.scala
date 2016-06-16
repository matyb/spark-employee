package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter

case class Employee(id: Long, dob: Date, firstName: String, lastName: String, gender: Gender.Value, hireDate: Date) 

object Employee extends Converter[Array[String],Employee] {
  
  def convert(array: Array[String]): Employee = {
    Employee(array(0).toLong,
             new SimpleDateFormat("yyyy-MM-dd").parse(array(1).replace("'","")), 
             array(2), 
             array(3), 
             Gender withName array(4), 
             new SimpleDateFormat("yyyy-MM-dd").parse(array(5).replace("'","")))
  }
  
} 