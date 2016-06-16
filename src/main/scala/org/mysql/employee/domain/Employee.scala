package org.mysql.employee.domain

import java.util.Date
import org.mysql.employee.enums.Gender
import java.text.SimpleDateFormat

case class Employee(id: Long, dob: Date, firstName: String, lastName: String, gender: Gender.Value, hireDate: Date) 

object Employee {
  
  def fromArray(array: Array[String]): Employee = {
    Employee(array(0).toLong,
             new SimpleDateFormat("yyyy-MM-dd").parse(array(1).replace("'","")), 
             array(2), 
             array(3), 
             Gender withName array(4), 
             new SimpleDateFormat("yyyy-MM-dd").parse(array(5).replace("'","")))
  }
  
} 