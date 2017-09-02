package org.mysql.employee.aggregator

import java.util.Date

import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.EmployeeAsOf
import org.mysql.employee.enums.Gender

case class RddEmployeeAggregate (asOfDate: Date, employeesByGenderAndDepartment: RDD[(GroupBy, Iterable[EmployeeAsOf])]) extends EmployeeAggregate {
  
			
  def this(employees: RDD[Employee], asOfDate: Date) = 
    this(asOfDate, employees.filter { employee => (employee.isEmployedAsOf(asOfDate)) }
                            .map(_.asOf(asOfDate))
                            .groupBy{ emp => GroupBy(emp.employeeDemographic.gender, emp.departmentEmployee._2) }.cache())
                  
  val active = employeesByGenderAndDepartment.flatMap(_._2)
  val managers = active.filter { employee => employee.departmentManaged != None}
                                                     .groupBy { manager => manager.departmentManaged.get._2 }.cache()
  
  def activeCount() = {
    employeesByGenderAndDepartment.map{ case (groupBy, employees) => (groupBy, employees.size.toLong) }
                                  .reduceByKey(_ + _).collectAsMap().toMap
  }
  
  def managersByDepartment() = {
    managers.collectAsMap().toMap
  }
  
  def salaryByDepartment() = {
    new RddSalariesAggregate(employeesByGenderAndDepartment)
  }
  
}

case class GroupBy(gender: Gender.Value, department: Department) 