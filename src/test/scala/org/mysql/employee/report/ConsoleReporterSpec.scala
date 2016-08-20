package org.mysql.employee.report

import java.util.Date

import org.mysql.employee.aggregator.EmployeeAggregate
import org.mysql.employee.aggregator.GroupBy
import org.mysql.employee.aggregator.SalariesAggregate
import org.mysql.employee.aggregator.SalariesAggregate
import org.mysql.employee.domain.Department
import org.mysql.employee.utils.DateUtils._
import org.scalamock.MockFactoryBase
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext
import org.mysql.employee.enums.Gender
import org.mysql.employee.domain.EmployeeAsOf
import org.mysql.employee.TestData

class ConsoleReporterSpec extends FunSpec with Matchers with MockFactory with BeforeAndAfter {

  val outputSdf = outputFormat()
  var data: TestData = _

  before {
    data = new TestData()
  }

  describe("the header") {

    it("should contain as of date") {
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

  describe("count of employed") {
    it("should contain a count of active employees") {
      val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.F, data.oneDepartment) -> 1, GroupBy(Gender.F, data.twoDepartment) -> 1, 
                                                         GroupBy(Gender.M, data.oneDepartment) -> 2, GroupBy(Gender.M, data.twoDepartment) -> 1))
      ConsoleReporter.report(aggregate) should include(
        s"""
--- Number employed: 5 (3 - M, 2 - F)""")
    }

    it("should contain a count of active employees when no one is employed") {
      val aggregate = createAggregator(activeCount = Map())
      ConsoleReporter.report(aggregate) should include(
        s"""
--- Number employed: 0 (0 - M, 0 - F)""")
    }

    it("should contain a count of active employees with commas in every 3 places") {
      val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.F, data.oneDepartment) -> 9999, GroupBy(Gender.M, data.twoDepartment) -> 8888))
      ConsoleReporter.report(aggregate) should include(
        s"""
--- Number employed: 18,887 (8,888 - M, 9,999 - F)""")
    }

    describe("by department") {
          it("should contain a count of active employees by gender and department") {
            val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.F, data.oneDepartment) -> 2, 
                                                               GroupBy(Gender.M, data.oneDepartment) -> 1, 
                                                               GroupBy(Gender.M, data.twoDepartment) -> 2))
            ConsoleReporter.report(aggregate) should include(
              s"""
Department               # Employed:
====================================
Department1              3 (1 - M, 2 - F)
Department2              2 (2 - M)""")
          }
      
          it("should contain a count of active employees when no one is employed ") {
            val aggregate = createAggregator(activeCount = Map())
            ConsoleReporter.report(aggregate) should include(
              s"""
Department               # Employed:
====================================""")
          }
      
          it("should contain a count of active employees with commas in every 3 places") {
            val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.F, data.oneDepartment) -> 9999, 
                                                               GroupBy(Gender.M, data.oneDepartment) -> 8888))
            ConsoleReporter.report(aggregate) should include(
              s"""
Department               # Employed:
====================================
Department1              18,887 (8,888 - M, 9,999 - F)""")
          }
    }
    
  }

  describe("managers") {
    it("can print no departments") {
      val aggregate = createAggregator(managersByDepartment = Map())
      ConsoleReporter.report(aggregate) should include(
        s"""
Department               Manager(s):
====================================
""")
    }

    it("can print a department with a manager") {
      val aggregate = createAggregator(managersByDepartment = Map(data.oneDepartment -> List(data.manager3.asOf(data.asOfDate))))
      ConsoleReporter.report(aggregate) should include(
        """
Department1              Facello, Manager3(F)
""")
    }

    it("can print a department with multiple manager") {
      val aggregate = createAggregator(managersByDepartment =
        Map(data.twoDepartment -> List(
          data.manager1.asOf(data.asOfDate), data.manager2.asOf(data.asOfDate))))
      ConsoleReporter.report(aggregate) should include(
        """
Department2              Facello, Manager1(M); Facello, Manager2(M)
""")
    }

    it("can print a multiple departments with managers") {
      val aggregate = createAggregator(managersByDepartment = Map(
        data.twoDepartment -> List(data.manager1.asOf(data.asOfDate), data.manager2.asOf(data.asOfDate)),
        Department("id1", "Department1") -> List(data.manager3.asOf(data.asOfDate))))
      ConsoleReporter.report(aggregate) should include(
        """
Department               Manager(s):
====================================
Department2              Facello, Manager1(M); Facello, Manager2(M)
Department1              Facello, Manager3(F)
""")
    }
  }

  describe("salaries") {

    describe("average by department") {

      it("can print heading with no departments") {
        val aggregate = createAggregator(averageSalaries = Map())
        ConsoleReporter.report(aggregate) should include(
          s"""
Department               Avg Salary:
====================================

""")
      }

      it("can print heading with one department") {
        val department = Department("id1", "Department1")
        val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.M, department) -> 1, GroupBy(Gender.F, department) -> 2),
                                         averageSalaries = Map(GroupBy(Gender.M, department) -> 41000,
                                                               GroupBy(Gender.F, department) -> 28000))
        ConsoleReporter.report(aggregate) should include(
"""
Department               Avg Salary:
====================================
Department1              $32,333 ($41,000 - M, $28,000 - F)
""")
      }

      it("can print heading with multiple departments") {
        val departmentOne = Department("id1", "Department1")
        val departmentTwo = Department("id2", "Department2")
        val aggregate = createAggregator(activeCount = Map(GroupBy(Gender.M, departmentOne) -> 1, GroupBy(Gender.F, departmentOne) -> 1, GroupBy(Gender.F, departmentTwo) -> 1),
                                         averageSalaries = Map(GroupBy(Gender.M, departmentOne) -> 41000,
                                                               GroupBy(Gender.F, departmentOne) -> 28000,
                                                               GroupBy(Gender.F, departmentTwo) -> 78533))
        ConsoleReporter.report(aggregate) should include(
"""
Department               Avg Salary:
====================================
Department2              $78,533 ($0 - M, $78,533 - F)
Department1              $34,500 ($41,000 - M, $28,000 - F)
""")
      }

    }

    describe("max by department") {

      it("can print heading with no departments") {
        val aggregate = createAggregator(maxSalaries = Map())
        ConsoleReporter.report(aggregate) should include(
          s"""
Department               Max Salary:
====================================

""")
      }

      it("can print heading with one department") {
        val aggregate = createAggregator(maxSalaries = Map(GroupBy(Gender.M, Department("id1", "Department1")) -> 39500))
        ConsoleReporter.report(aggregate) should include(
          s"""
Department               Max Salary:
====================================
Department1              39500
""")
      }

      it("can print heading with multiple departments") {
        val aggregate = createAggregator(maxSalaries = Map(GroupBy(Gender.M, Department("id1", "Department1")) -> 39500,
          GroupBy(Gender.M, data.twoDepartment) -> 39500,
          GroupBy(Gender.F, data.twoDepartment) -> 78533))
        ConsoleReporter.report(aggregate) should include(
          s"""
Department               Max Salary:
====================================
Department1              39500
Department2              78533
""")
      }

    }

  }

  def createAggregator(asOfDate: Date = new Date(), averageSalaries: Map[GroupBy, Long] = Map(), maxSalaries: Map[GroupBy, Long] = Map(),
                       activeCount: Map[GroupBy,Long] = Map(), managersByDepartment: Map[Department, List[EmployeeAsOf]] = Map()): EmployeeAggregate = {
    val employee = mock[EmployeeAggregate]
    val salaries = mock[SalariesAggregate]
    employee.expects('asOfDate)().returning(asOfDate)
    employee.expects('salaryByDepartment)().returning(salaries)
    employee.expects('activeCount)().returning(activeCount)
    employee.expects('managersByDepartment)().returning(managersByDepartment)
    salaries.expects('averages)().returning(averageSalaries)
    salaries.expects('maximums)().returning(maxSalaries)
    employee
  }

}