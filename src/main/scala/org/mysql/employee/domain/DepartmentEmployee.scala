package org.mysql.employee.domain

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.utils.Converter

case class DepartmentEmployee(employee: EmployeeDemographic, department: Department, starting: Date, ending: Date)

object DepartmentEmployee extends Converter[(EmployeeDemographic,Department,String,String), DepartmentEmployee] {
  
  def convert(args: (EmployeeDemographic,Department,String,String)): DepartmentEmployee = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    DepartmentEmployee(args._1, args._2, sdf.parse(args._3), sdf.parse(args._4))
  }
  
} 