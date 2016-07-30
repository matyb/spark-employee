package org.mysql.employee.report

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.Department
import org.mysql.employee.aggregator.EmployeeAggregate
import scala.collection.SeqLike

object ConsoleReporter extends Reporter[String]{

  def sdf = new SimpleDateFormat(DateConstants.outputDateFormat)
  
  def report(aggregate: EmployeeAggregate) = {
    val asOfString = sdf.format(aggregate.asOfDate)
    val salaryByDepartment = aggregate.salaryByDepartment()
    val separator = "\n"
    val formatMap = (map : Map[String,_]) => 
      map.foldLeft(""){(x,y) =>
        x + s"${y._1.padTo(25, ' ')}${y._2}$separator"  } 
    val formatMapOfLists = (map : Map[String, List[String]]) =>
      formatMap(map.foldLeft(Map.empty[String,String]){
        (result: Map[String,String], e: (String, List[String])) => 
          result + (e._1 -> e._2.mkString("; "))})                                
s"""
Report as of: '$asOfString'
==========================
--- Number employed: ${aggregate.activeCount()}

Department               Manager(s):
====================================
""" + 
formatMapOfLists(aggregate.managersByDepartment()) +"""
  
Department               Avg Salary:
====================================
""" + 
formatMap(salaryByDepartment.averageByDepartment()) +"""
"""
  }
  
}