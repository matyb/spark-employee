package org.mysql.employee.aggregator

import scala.collection.Map

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.EmployeeAsOf
import org.mysql.employee.enums.Gender

case class RddSalariesAggregate(averages: Map[GroupBy, Long], maximums: Map[GroupBy, Long]) extends SalariesAggregate {
  
  def this(employeesByDepartment: RDD[(GroupBy, Iterable[EmployeeAsOf])]) = this(
      employeesByDepartment.map { case (groupBy, employees) => (groupBy -> employees.foldLeft(0L)
          (_ + _.employeeSalary.salaryDollars) / employees.size)}.collectAsMap(),
      employeesByDepartment.map { case (groupBy, employees) => (groupBy -> employees.foldLeft(0L)
          (_ max _.employeeSalary.salaryDollars))}.collectAsMap()
  )
  
}