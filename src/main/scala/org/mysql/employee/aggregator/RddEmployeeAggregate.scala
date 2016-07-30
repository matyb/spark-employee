package org.mysql.employee.aggregator

import java.util.Date
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.EmployeeAsOf

case class RddEmployeeAggregate (asOfDate: Date, employeesByDepartment: RDD[(String, Iterable[EmployeeAsOf])]) extends EmployeeAggregate {
  
			
  def this(employees: RDD[Employee], asOfDate: Date) = 
    this(asOfDate, employees.map { employee => employee.filter(asOfDate) }
                            .map { employee => (employee.isEmployedAsOf(asOfDate), employee ) }
                            .filter(_._1).map(_._2).groupBy{ emp => emp.department.name }.cache())
                  
  val active = employeesByDepartment.flatMap(_._2)
  val managers = active.filter { employee => employee.managedDepartment != Department.UNKNOWN }.cache()
  val departmentManagers = managers.groupBy { manager => manager.managedDepartment.name }
  
  def activeCount() = {
    active.count()
  }
  
  def managersByDepartment() = {
    departmentManagers.collect().foldLeft(Map.empty[String,List[String]]){ (result, mgr) =>
      result + (mgr._1 -> mgr._2.map{ employee =>
        val demographic = employee.employeeDemographic
        demographic.lastName + ", " + demographic.firstName
      }.toList)
    }
  }
  
  def salaryByDepartment() = {
    new RddSalariesAggregate(employeesByDepartment)
  }
  
}