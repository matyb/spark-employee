package org.mysql.employee.report

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.Department

object ConsoleReporter {

  def sdf = new SimpleDateFormat(DateConstants.outputDateFormat)
  
  def report(employees: RDD[Employee]) = {
    new Reporter[String] {
      def asOf(asOfDate: Date) = {
    	  val asOfString = sdf.format(asOfDate)
        val filtered = employees.map { employee => employee.filter(asOfDate)}
                                .map { employee => (employee.isEmployedAsOf(asOfDate), employee )}.cache()
                                
        val active = filtered.filter(_._1).map(_._2).cache()
        val inactive = filtered.filter(!_._1).map(_._2).cache()
        filtered.unpersist(false) // Don't use filtered anymore
        
        val managers = active.filter { employee => employee.managedDepartment != Department.UNKNOWN }
        val managersByDepartment = managers.groupBy { manager => manager.managedDepartment.name }
                                
s"""
Report as of: '$asOfString'
==========================
--- Number employed: ${active.count()}

Department               Manager(s):
====================================
""" + 
managersByDepartment.map{ mgr => 
  mgr._1.padTo(25, ' ') + mgr._2.map {(employee => {
    val demographic = employee.employeeDemographic
    demographic.lastName + ", " + demographic.firstName
  })}.mkString("; ")
}.collect().mkString("\n") + "\n"
//        .map { ((departmentName : String, employeeNames : Iterable[String])) =>
//          departmentName + employeeNames.mkString(";")
//        }
      }
    }
  }
  
}