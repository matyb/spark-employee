package org.mysql.employee.report

import org.mysql.employee.utils.DateUtils._

import java.util.Date

import org.apache.spark.rdd.RDD
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.Department
import org.mysql.employee.aggregator.EmployeeAggregate
import scala.collection.SeqLike
import scala.collection.Map
import org.mysql.employee.enums.Gender
import org.mysql.employee.aggregator.GroupBy

object ConsoleReporter extends Reporter[String]{

  def sdf = outputFormat()
  
  def report(aggregate: EmployeeAggregate) = {
    val asOfString = sdf.format(aggregate.asOfDate)
    val salaryByDepartment = aggregate.salaryByDepartment()
    val active = aggregate.activeCount().toSeq.sortBy( x => (x._1.department.name, x._1.gender) )
    val activeByGender = active.foldLeft(Map.empty[Gender.Value, Long]){ (result, emp) =>
      val sum = result.getOrElse(emp._1.gender, 0L) + emp._2
      result + ((emp._1.gender, sum))  
    };
    val longFormat = java.text.NumberFormat.getIntegerInstance.format(_ : Long)
    val countText = s"${longFormat(activeByGender.values.foldLeft(0L)(_ + _))}" + 
                      s" (${longFormat(activeByGender.getOrElse(Gender.F, 0L))} - ${Gender.F}" +
                      s", ${longFormat(activeByGender.getOrElse(Gender.M, 0L))} - ${Gender.M})"
    val separator = "\n"
    val formatMap = (map : Map[String,Iterable[_]]) => 
      map.foldLeft(""){(x,y) =>
        x + s"${y._1.padTo(25, ' ')}${y._2.mkString("; ")}$separator"  }
    val formatDepartmentCount = (map : Map[Department, Map[Gender.Value, Long]]) =>
      map.foldLeft(""){(x,y) =>
        x + s"${y._1.name.padTo(25, ' ')}${longFormat(y._2.values.foldLeft(0L)(_ + _))} (${y._2.map { count => longFormat(count._2) + " - " + count._1 }.mkString(", ")})$separator"  }
                                    
s"""
Report as of: '$asOfString'
==========================
--- Number employed: """ + countText + """

Department               Manager(s):
====================================
""" + 
formatMap(aggregate.managersByDepartment().map { case(x,y) => 
  (x.name, y.map { z => s"${z.employeeDemographic.lastName}, ${z.employeeDemographic.firstName}(${z.employeeDemographic.gender})"} ) 
}) +"""
  
Department               # Employed:
====================================
""" + 
formatDepartmentCount(active.foldLeft(Map.empty[Department,Map[Gender.Value,Long]]) { case(result, employee) => 
  val employeesByGender = result.getOrElse(employee._1.department, Map())
  val sum = employeesByGender.getOrElse(employee._1.gender, 0L)
  val m = employeesByGender + (employee._1.gender -> (sum + employee._2))
  result + (employee._1.department -> m)
}) +"""
  
Department               Avg Salary:
====================================
""" + 
formatMap(salaryByDepartment.averages().map { case(x,y) => (x.department.name, List(y)) } ) +"""
  
Department               Max Salary:
====================================
""" + 
formatMap(salaryByDepartment.maximums().map { case(x,y) => (x.department.name, List(y)) } ) +"""
"""
  }
  
}