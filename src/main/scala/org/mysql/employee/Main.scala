package org.mysql.employee

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.mysql.employee.constants.DateConstants
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.domain.EmployeeSalary
import org.mysql.employee.domain.EmployeeTitle
import org.mysql.employee.utils.FileUtils.rmFolder
import org.mysql.employee.report.ConsoleReporter
import java.util.Date
import org.mysql.employee.aggregator.EmployeeAggregate
import org.mysql.employee.aggregator.RddEmployeeAggregate

object Main {

  val logger = Logger.getLogger(this.getClass())

  def main(arg: Array[String]) {

    validateArgs(logger, arg)

    val jobName = "Employee DB"

    val sc = createContext(jobName)

    val (pathToFiles, outputPath) = (arg(0), arg(1))

    rmFolder(outputPath)

    logger.info(s"=> jobName  $jobName ")
    logger.info(s"=> pathToFiles $pathToFiles ")
    
    class Parser(sc: SparkContext, pathToFiles: String) {
      def apply[T:ClassTag](fileName: String, converter: (Array[String],SimpleDateFormat) => T) : RDD[T] = {
        parse(sc.textFile(s"$pathToFiles/$fileName"), converter) 
      }
    }
    
    def parseFile = new Parser(sc, pathToFiles)
    
    val employeeDemographics = parseFile("load_employees.dump",    EmployeeDemographic(_,_))
    val departments          = parseFile("load_departments.dump",  Department(_,_))
    val departmentEmployees  = parseFile("load_dept_emp.dump",     DepartmentEmployee(_,_))
    val departmentManagers   = parseFile("load_dept_manager.dump", DepartmentManager(_,_))
    val employeeTitles       = parseFile("load_titles.dump",       EmployeeTitle(_,_))
    val employeeSalaries     = parseFile("load_salaries1.dump",    EmployeeSalary(_,_)).union(
                               parseFile("load_salaries2.dump",    EmployeeSalary(_,_)).union(
                               parseFile("load_salaries3.dump",    EmployeeSalary(_,_))))                           

    val employees = join(departments, departmentEmployees, departmentManagers, 
                         employeeDemographics, employeeTitles, employeeSalaries).cache()
                             
    println(ConsoleReporter.report(new RddEmployeeAggregate(employees, new Date())))
  }
  
  def validateArgs(logger: Logger, arg: Array[String]) = {
    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: Main <path-to-files> <output-path>")
      System.exit(1)
    }
  }

  def createContext(jobName: String): SparkContext = {
    val conf = new SparkConf().setAppName(jobName).setMaster("local")
    new SparkContext(conf)
  }

  def parse(lines: RDD[String]) = {
    lines.map(_.trim.replaceAll("(INSERT INTO `.*` VALUES\\s*)|\\(|'|\\),|\\)|;", "")).filter(!_.isEmpty)
  }
  
  def parse[T: ClassTag](rdd: RDD[String], converter: (Array[String],SimpleDateFormat) => T): RDD[T] = {
    parse(rdd).map { line => converter(line.split(","), new SimpleDateFormat(DateConstants.ingestionDateFormat)) }
  }

  def join(departments: RDD[Department], departmentEmployees: RDD[DepartmentEmployee], departmentManagers: RDD[DepartmentManager], employeeDemographics: RDD[EmployeeDemographic], employeeTitles: RDD[EmployeeTitle], employeeSalaries: RDD[EmployeeSalary]) = {
    val departmentsRdd = departments.map { row => (row.id, row) }
    val departmentEmployeesDepKeyRdd = departmentsRdd.join(departmentEmployees.map { row => (row.departmentId, row) })
    val departmentEmployeesEmpKeyRdd = departmentEmployeesDepKeyRdd.map { row => (row._2._2.employeeId, row._2) }
    val departmentManagerDepRdd = departmentsRdd.join(departmentManagers.map { row => (row.managedDepartmentId, row) })
                                                .map{ row => (row._2._2.employeeId, (row._2._2, row._2._1)) }
    val employeeDemographicsRdd = employeeDemographics.map { row => (row.employeeId, row )}
                                                      .leftOuterJoin(departmentManagerDepRdd)
    
    val grouped = departmentEmployeesEmpKeyRdd
                    .join(employeeDemographicsRdd
                        .join(employeeSalaries.map { row => (row.employeeId, row) })
                        .join(employeeTitles.map { row => (row.employeeId, row) } )).groupBy { row => row._1 }
    
    grouped.map { row =>
      val departmentEmployee = ListBuffer[(DepartmentEmployee, Department)]()
      val departmentManager = ListBuffer[(DepartmentManager, Department)]()
      val employeeDemographic = ListBuffer[EmployeeDemographic]()
      val employeeTitles = ListBuffer[EmployeeTitle]()
      val employeeSalaries = ListBuffer[EmployeeSalary]()
      var id : String = ""
      row._2.foreach { values =>
        id = values._1
        departmentEmployee += ((values._2._1._2, values._2._1._1))
        if(values._2._2._1._1._2 != None){
          departmentManager += ((values._2._2._1._1._2.get))
        }
        employeeDemographic += values._2._2._1._1._1
        employeeTitles += values._2._2._2
        employeeSalaries += values._2._2._1._2
      }
      if (id == "") throw new RuntimeException("Employee with no records")
      Employee(id,
          departmentEmployee.toList, 
          departmentManager.toList, 
          employeeDemographic.toList,
          employeeTitles.toList, 
          employeeSalaries.toList)
    }
  }

}
