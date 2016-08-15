package org.mysql.employee

import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.mysql.employee.aggregator.RddEmployeeAggregate
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.Employee
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.domain.EmployeeSalary
import org.mysql.employee.domain.EmployeeTitle
import org.mysql.employee.report.ConsoleReporter
import org.mysql.employee.utils.DateUtils.ingestionFormat
import org.mysql.employee.utils.DateUtils.outputTimeFormat
import org.mysql.employee.utils.DateUtils.toHumanTime
import org.mysql.employee.utils.FileUtils.rmFolder


object Main {

  val logger = Logger.getLogger(this.getClass())

  def main(arg: Array[String]) {

    val start = System.currentTimeMillis()
    
    validateArgs(logger, arg)

    val jobName = "Employee DB"

    val sc = createContext(jobName)

    val (pathToFiles, outputPath) = (arg(0), arg(1))
    val fsOutput = s"$outputPath/tmp"
    val reportDir = s"$outputPath/reports";
    
    rmFolder(fsOutput)
    new File(reportDir).mkdirs()

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
    
    report(employees, start, reportDir)
  }
  
  def report(employees: RDD[Employee], start: Long, reportDir: String){
    val report = ConsoleReporter.report(new RddEmployeeAggregate(employees, new Date()))   
    val now = System.currentTimeMillis()
    val endTimeString = outputTimeFormat().format(new Date())
    val times = s"${toHumanTime(now - start)} to generate, ended @$endTimeString"
    
    println(report)
    println(times)
    
    new PrintWriter(s"$reportDir/report-$now.log") { write(times); close }
    new PrintWriter(s"$reportDir/report-$now.txt") { write(report); close }
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
    parse(rdd).map { line => converter(line.split(","), ingestionFormat()) }
  }

  def join(departments: RDD[Department], departmentEmployees: RDD[DepartmentEmployee], departmentManagers: RDD[DepartmentManager], employeeDemographics: RDD[EmployeeDemographic], employeeTitles: RDD[EmployeeTitle], employeeSalaries: RDD[EmployeeSalary]) = {
    val departmentsRdd = departments.map { row => (row.id, row) }
    
    val departmentEmployeesDepKeyRdd = departmentsRdd.join(departmentEmployees.map { row => (row.departmentId, row) })
    val departmentEmployeesEmpKeyRdd = departmentEmployeesDepKeyRdd.map { case (departmentId ,(department, departmentEmployee)) =>
      (departmentEmployee.employeeId, (department, departmentEmployee)) 
    }
    val departmentManagerDepRdd = departmentsRdd.join(departmentManagers.map { row => (row.managedDepartmentId, row) })
                                                .map{ case (departmentId, (managedDepartment, departmentManager)) =>
                                                  (departmentManager.employeeId, (departmentManager, managedDepartment)) 
                                                }
    
    val grouped = departmentEmployeesEmpKeyRdd
                    .join(employeeDemographics.map { demographic => (demographic.employeeId, demographic )}
                    .leftOuterJoin(departmentManagerDepRdd)
                    .join(employeeSalaries.map { salary => (salary.employeeId, salary) })
                    .join(employeeTitles.map { title => (title.employeeId, title) } ))
                    .groupBy { case (empId, _) => 
                      empId
                    }
    
    grouped.map { case (key, records) =>
      val departmentEmployees = ListBuffer[(DepartmentEmployee, Department)]()
      val departmentManagers = ListBuffer[(DepartmentManager, Department)]()
      val employeeDemographics = ListBuffer[EmployeeDemographic]()
      val employeeTitles = ListBuffer[EmployeeTitle]()
      val employeeSalaries = ListBuffer[EmployeeSalary]()
      records.foreach { case (empId, ((department, departmentEmployee), (((demographic, manager), salary), title))) =>
        departmentEmployees += ((departmentEmployee, department))
        departmentManagers ++= manager
        employeeDemographics += demographic
        employeeTitles += title
        employeeSalaries += salary
      }
      if (key == "") throw new RuntimeException("Employee with no records")
      
      Employee(key,
              departmentEmployees.toList,
              departmentManagers.toList,
              employeeDemographics.toList,
              employeeTitles.toList, 
              employeeSalaries.toList)
    }
  }

}
