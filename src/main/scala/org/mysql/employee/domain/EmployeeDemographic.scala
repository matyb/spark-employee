package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter

case class EmployeeDemographic(id: Long, dob: Date, firstName: String, lastName: String, gender: Gender.Value, hireDate: Date) 

object EmployeeDemographic extends Converter[Array[String],EmployeeDemographic] {
  
  def convert(array: Array[String]): EmployeeDemographic = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    EmployeeDemographic(array(0).toLong,
             sdf.parse(array(1).replace("'","")), 
             array(2), 
             array(3), 
             Gender withName array(4), 
             sdf.parse(array(5).replace("'","")))
  }
  
} 