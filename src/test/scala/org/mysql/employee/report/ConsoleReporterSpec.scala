package org.mysql.employee.report

import java.text.SimpleDateFormat
import java.util.Date

import org.mysql.employee.aggregator.EmployeeAggregate
import org.mysql.employee.constants.DateConstants
import org.scalamock.MockFactoryBase
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalamock.scalatest.proxy.MockFactory
import org.mysql.employee.aggregator.SalariesAggregate
import org.mysql.employee.aggregator.RddSalariesAggregate
import org.mysql.employee.aggregator.SalariesAggregate


class ConsoleReporterSpec extends FunSpec with Matchers with MockFactory {
  
  val outputSdf = new SimpleDateFormat(DateConstants.outputDateFormat)
  
  describe("the header") {
        
    it ("should contain as of date") {
      val dateString = "12/31/2010"
      val date = outputSdf.parse(dateString)
      val aggregate = createAggregator(asOfDate = date)
      ConsoleReporter.report(aggregate) should include(
s"""
Report as of: '$dateString'
==========================
""")
    }
    
  }
  
  describe("count of employed"){
    it ("should contain a count of active employees") {
      val aggregate = createAggregator(activeCount = 5)
      ConsoleReporter.report(aggregate) should include(
s"""
--- Number employed: 5""")
    }
    
    it ("should contain a count of active employees when no one is employed") {
      val aggregate = createAggregator(activeCount = 0)
      ConsoleReporter.report(aggregate) should include(
s"""
--- Number employed: 0""")
    }
    
  }
  
  describe("managers"){
    it ("can print no departments") {
      val aggregate = createAggregator(managersByDepartment = Map())
      ConsoleReporter.report(aggregate) should include(
s"""
Department               Manager(s):
====================================
""")
    }
    
    it ("can print a department with a manager") {
      val aggregate = createAggregator(managersByDepartment = Map("Department1" -> List("Facello, Manager3")))
      ConsoleReporter.report(aggregate) should include(
"""
Department1              Facello, Manager3
""")
    }
    
    it ("can print a department with multiple manager") {
      val aggregate = createAggregator(managersByDepartment = Map("Department2" -> List("Facello, Manager1", "Facello, Manager2")))
      ConsoleReporter.report(aggregate) should include(
"""
Department2              Facello, Manager1; Facello, Manager2
""")
    }
    
    it ("can print a multiple departments with managers") {
      val aggregate = createAggregator(managersByDepartment = Map(
          "Department2" -> List("Facello, Manager1", "Facello, Manager2"), 
          "Department1" -> List("Facello, Manager3")) )
      ConsoleReporter.report(aggregate) should include(
"""
Department               Manager(s):
====================================
Department2              Facello, Manager1; Facello, Manager2
Department1              Facello, Manager3
""")
    }
  }
  
  describe("salaries") {
    
    describe("by department"){

      it ("can print heading with no departments") {
        val aggregate = createAggregator(averageSalaries = Map())
        ConsoleReporter.report(aggregate) should include(
s"""
Department               Avg Salary:
====================================

""")
      }
      
      it ("can print heading with one department") {
        val aggregate = createAggregator(averageSalaries = Map("Department1" -> 39500))
        ConsoleReporter.report(aggregate) should include(
s"""
Department               Avg Salary:
====================================
Department1              39500
""")
      }
      
      it ("can print heading with multiple departments") {
        val aggregate = createAggregator(averageSalaries = Map("Department1" -> 39500, "Department2" -> 78533))
        ConsoleReporter.report(aggregate) should include(
s"""
Department               Avg Salary:
====================================
Department1              39500
Department2              78533
""")
      }
      
    }
    
  }
 
  def createAggregator(asOfDate: Date = new Date(), averageSalaries: Map[String, Int] = Map(), 
      activeCount: Int = 0, managersByDepartment: Map[String,List[String]] = Map()) : EmployeeAggregate = {
    val employee = mock[EmployeeAggregate]
    val salaries = mock[SalariesAggregate]
    employee.expects('asOfDate)().returning(asOfDate)
    employee.expects('salaryByDepartment)().returning(salaries)
    employee.expects('activeCount)().returning(activeCount)
    employee.expects('managersByDepartment)().returning(managersByDepartment)
    salaries.expects('averageByDepartment)().returning(averageSalaries)
    employee
  }
  
}