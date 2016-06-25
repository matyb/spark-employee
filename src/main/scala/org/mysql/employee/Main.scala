package org.mysql.employee

import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.domain.EmployeeSalary
import org.mysql.employee.domain.EmployeeTitle
import org.mysql.employee.utils.Converter
import org.mysql.employee.utils.FileUtils.rmFolder

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

    val employeeDemographics = parse(sc.textFile(s"$pathToFiles/load_employees.dump"), EmployeeDemographic)
    val departments = parse(sc.textFile(s"$pathToFiles/load_departments.dump"), Department)
    val departmentEmployees = parse(sc.textFile(s"$pathToFiles/load_dept_emp.dump"), DepartmentEmployee)
    val departmentManagers = parse(sc.textFile(s"$pathToFiles/load_dept_manager.dump"), DepartmentManager)
    val employeeTitles = parse(sc.textFile(s"$pathToFiles/load_titles.dump"), EmployeeTitle)
    val employeeSalaries = parse(sc.textFile(s"$pathToFiles/load_salaries1.dump"), EmployeeSalary).union(
                           parse(sc.textFile(s"$pathToFiles/load_salaries2.dump"), EmployeeSalary).union(
                           parse(sc.textFile(s"$pathToFiles/load_salaries3.dump"), EmployeeSalary)))                           

    employeeDemographics.saveAsTextFile(s"$outputPath/employee_demographics")
    departments.saveAsTextFile(s"$outputPath/departments")
    departmentEmployees.saveAsTextFile(s"$outputPath/department_employees")
    departmentManagers.saveAsTextFile(s"$outputPath/department_managers")
    employeeTitles.saveAsTextFile(s"$outputPath/employee_titles")
    employeeSalaries.saveAsTextFile(s"$outputPath/employee_salaries")
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

  def parse[T: ClassTag](rdd: RDD[String], converter: Converter[Array[String], T]): RDD[T] = {
    val convert = converter.convert(_)
    parse(rdd).map { line => convert(line.split(",")) }
  }

}
