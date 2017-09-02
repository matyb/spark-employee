package org.mysql.employee.aggregator

import org.mysql.employee.TestData
import org.scalatest.BeforeAndAfter
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.Matchers
import org.scalatest.FunSpec
import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.DateUtils._

class RddEmployeeAggregateSpec extends FunSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  
  var data : TestData = _
  
  before {
    data = new TestData()
  }
  
  describe("supplied values") {
    
    it ("should contain as of date") {
      new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).asOfDate should equal(data.asOfDate)
    }
    
  }
  
  describe("count of employed"){
    it ("should contain a count of active employees") {
      new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).activeCount() should equal(
          Map(GroupBy(Gender.M, data.twoDepartment) -> 1, GroupBy(Gender.F, data.twoDepartment) -> 1,
              GroupBy(Gender.F, data.oneDepartment) -> 1, GroupBy(Gender.M, data.oneDepartment) -> 2))
    }
    
    it ("should contain a count of active employees before anyone is employed") {
      val asOfDate = outputFormat().parse("09/01/1983")
      new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).activeCount() should equal(Map())
    }

    it ("should contain a count of active employees with first employee") {
      val asOfDate = outputFormat().parse("09/02/1983")
      new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).activeCount() should equal(Map(GroupBy(Gender.M, data.oneDepartment) -> 1))
    }
    
    it ("should contain a count of active employees on last day of last employee") {
      val asOfDate = outputFormat().parse("03/03/2013")
      new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).activeCount() should equal(Map(GroupBy(Gender.F, data.twoDepartment) -> 1))
    }
    
    it ("should contain a count of active employees after last day of last employee") {
      val asOfDate = outputFormat().parse("03/04/2013")
      new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).activeCount() should equal(Map())
    }    
  }
  
  describe("managers"){
    it ("with no departments") {
      val asOfDate = outputFormat().parse("01/01/0001")
      new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).managersByDepartment() should equal(Map())
    }
    
    it ("a department with a manager") {
      new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).
        managersByDepartment().get(data.oneDepartment) should equal(
            Some(Iterable(data.manager3.asOf(data.asOfDate))))
    }
    
    it ("a department with multiple managers") {
      new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).
        managersByDepartment().get(data.twoDepartment) should equal(
            Some(Iterable(data.manager1.asOf(data.asOfDate), data.manager2.asOf(data.asOfDate))))
    }
    
  }
  
  describe("salaries") {
    
    describe("average by department"){

      it ("no employees") {
        val asOfDate = outputFormat().parse("01/01/1980")
        new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).salaryByDepartment().averages should equal(Map())
      }
      
      it ("with employees") {
        val result = new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).salaryByDepartment().averages
        result.get(GroupBy(Gender.M,data.oneDepartment)) should equal(Some(85000))
        result.get(GroupBy(Gender.M,data.twoDepartment)) should equal(Some(34000))
        result.get(GroupBy(Gender.F,data.oneDepartment)) should equal(Some(65000))
        result.get(GroupBy(Gender.F,data.twoDepartment)) should equal(Some(45000))
      }      
      
    }
    
    describe("max by department"){

      it ("no employees") {
        val asOfDate = outputFormat().parse("01/01/1980")
        new RddEmployeeAggregate(sc.parallelize(data.employees), asOfDate).salaryByDepartment().maximums should equal(Map())
      }
      
      it ("with employees") {
        val result = new RddEmployeeAggregate(sc.parallelize(data.employees), data.asOfDate).salaryByDepartment().maximums
        result.get(GroupBy(Gender.M,data.oneDepartment)) should equal(Some(95000))
        result.get(GroupBy(Gender.M,data.twoDepartment)) should equal(Some(34000))
        result.get(GroupBy(Gender.F,data.oneDepartment)) should equal(Some(65000))
        result.get(GroupBy(Gender.F,data.twoDepartment)) should equal(Some(45000))
      }      
      
    }
    
  }
  
}