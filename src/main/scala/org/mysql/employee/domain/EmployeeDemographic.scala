package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.Converter
import org.mysql.employee.constants.DateConstants

case class EmployeeDemographic (employeeId: String, dob: Date, firstName: String, lastName: String, gender: Gender.Value, hireDate: Date) extends EmployeeId  

object EmployeeDemographic extends Converter[Array[String],EmployeeDemographic] {
  
  def convert(array: Array[String]): EmployeeDemographic = {
    val sdf = new SimpleDateFormat(DateConstants.ingestionDateFormat)
    EmployeeDemographic(array(0),
             sdf.parse(array(1).replace("'","")), 
             array(2), 
             array(3), 
             Gender withName array(4), 
             sdf.parse(array(5).replace("'","")))
  }
  
} 