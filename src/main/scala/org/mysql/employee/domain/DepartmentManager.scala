package org.mysql.employee.domain

import org.mysql.employee.utils.Converter
import java.text.SimpleDateFormat
import java.util.Date

case class DepartmentManager(employee: DepartmentEmployee, managerOf: Department, starting: Date, ending: Date)

object DepartmentManager extends Converter[(DepartmentEmployee,Department,String,String), DepartmentManager] {
  
  def convert(args: (DepartmentEmployee,Department,String,String)): DepartmentManager = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    DepartmentManager(args._1, args._2, sdf.parse(args._3), sdf.parse(args._4))
  }
  
} 