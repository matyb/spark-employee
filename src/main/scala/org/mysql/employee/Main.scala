package org.mysql.employee

import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.EmployeeDemographic
import org.mysql.employee.utils.Converter
import org.mysql.employee.utils.FileUtils.rmFolder
import org.mysql.employee.domain.DepartmentManager

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

    val employees = parse(sc.textFile(s"$pathToFiles/load_employees.dump"), EmployeeDemographic)
    val departments = parse(sc.textFile(s"$pathToFiles/load_departments.dump"), Department)
    val departmentEmployees = parseDepartmentEmployees(employees, departments, parse(sc.textFile(s"$pathToFiles/load_dept_emp.dump")))
    val departmentManagers = parseDepartmentManagers(departmentEmployees, departments, parse(sc.textFile(s"$pathToFiles/load_dept_manager.dump")))

    employees.saveAsTextFile(s"$outputPath/employees")
    departments.saveAsTextFile(s"$outputPath/departments")
    departmentEmployees.saveAsTextFile(s"$outputPath/department_employees")
    departmentManagers.saveAsTextFile(s"$outputPath/department_managers")
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

  def parseDepartmentEmployees(employeeRdd: RDD[EmployeeDemographic], departmentRdd: RDD[Department], departmentEmployeeRecords: RDD[String]): RDD[DepartmentEmployee] = {
    val employeeKeyed = employeeRdd.map { employee => (String.valueOf(employee.id), employee) }
    val departmentKeyed = departmentRdd.map { department => (department.id, department) }
    val rlshpDepartmentKeyed = departmentEmployeeRecords.keyBy { line => line.substring(line.indexOf(",") + 1, line.indexOf(",") + 5) }
    val joined = rlshpDepartmentKeyed.join(departmentKeyed).keyBy { t => t._2._1.substring(t._2._1.indexOf("(") + 1, t._2._1.indexOf(",")) }.join(employeeKeyed)

    joined.map { x =>
      val employee = x._2._2
      val department = x._2._1._2._2
      val rlshp = x._2._1._2._1
      val lastComma = rlshp.lastIndexOf(",")
      DepartmentEmployee.convert(employee, department, rlshp.substring(lastComma - 10, lastComma), rlshp.substring(lastComma + 1, lastComma + 11))
    }
  }

  def parseDepartmentManagers(departmentEmployeeRdd: RDD[DepartmentEmployee], departmentRdd: RDD[Department], departmentManagerRecords: RDD[String]): RDD[DepartmentManager] = {
    val departmentEmployeeKeyed = departmentEmployeeRdd.map { departmentEmployee => (String.valueOf(departmentEmployee.employee.id), departmentEmployee) }
    val departmentKeyed = departmentRdd.map { department => (department.id, department) }
    val rlshpEmployeeKeyed = departmentManagerRecords.map { line => (line.substring(0, line.indexOf(",")), line) }
    val rlshpDepartmentKeyed = departmentManagerRecords.map { line => (line.substring(line.indexOf(",") + 1, line.indexOf(",") + 5), line) }

    val joined = rlshpDepartmentKeyed
      .join(departmentKeyed).map { t => (t._2._1.substring(t._2._1.indexOf("(") + 1, t._2._1.indexOf(",")), t) } // employee id 
      .join(rlshpEmployeeKeyed).map { x => (x._1, x) }
      .join(departmentEmployeeKeyed)

    joined.map { x =>
      val departmentEmployee = x._2._2
      val managerOf = x._2._1._2._1._2._2
      val rlshp = x._2._1._2._2
      val lastComma = rlshp.lastIndexOf(",")
      DepartmentManager.convert(departmentEmployee, managerOf, rlshp.substring(lastComma - 10, lastComma), rlshp.substring(lastComma + 1, lastComma + 11))
    }
  }

}
