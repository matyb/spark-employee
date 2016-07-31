package org.mysql.employee.aggregator

import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.EmployeeAsOf

class RddSalariesAggregate(employeesByDepartment: RDD[(String, Iterable[EmployeeAsOf])]) extends SalariesAggregate {
  
  def averages(): Map[String, Long] = {
    employeesByDepartment.map { emp =>
      (emp._1, (emp._2.foldLeft(0L){ (a, b) => a + b.employeeSalary.salaryDollars } / emp._2.size))
    }.collect().foldLeft(Map.empty[String,Long]){ (result, tuple) => result + tuple}
  }
  
  def maximums(): Map[String, Long] = {
    employeesByDepartment.map { emp =>
      (emp._1 -> (emp._2.foldLeft(0l)(_ max _.employeeSalary.salaryDollars)))
    }.collect().foldLeft(Map.empty[String,Long]){ (result, tuple) => result + tuple}
  }
  
}