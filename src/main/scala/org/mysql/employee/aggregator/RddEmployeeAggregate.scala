package org.mysql.employee.aggregator

import java.util.Date
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.Department

case class RddEmployeeAggregate (employees: RDD[Employee], asOfDate: Date) extends EmployeeAggregate {
  
  val filtered = employees.map { employee => employee.filter(asOfDate) }
                          .map { employee => (employee.isEmployedAsOf(asOfDate), employee ) }.cache()
                          
  val active = filtered.filter(_._1).map(_._2).cache()
  val inactive = filtered.filter(!_._1).map(_._2).cache()
  filtered.unpersist(false) // Don't use filtered anymore
  
  val managers = active.filter { employee => employee.managedDepartment != Department.UNKNOWN }
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
    new RddSalariesAggregate(active)
  }
  
}