package org.mysql.employee

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Employee
import org.mysql.employee.enums.Gender
import org.mysql.employee.utils.FileUtils.rmFolder
import org.mysql.employee.domain.Department

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

    val employees = parseEmployees(sc.textFile(s"$pathToFiles/load_employees.dump"))
    val departments = parseDepartments(sc.textFile(s"$pathToFiles/load_departments.dump"))

    employees.saveAsTextFile(s"$outputPath/employees")
    departments.saveAsTextFile(s"$outputPath/departments")
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

  def parseEmployees(file: RDD[String]): RDD[Employee] = {
    cleanAndSplit(file).map { array => Employee.fromArray(array) }
  }
  
  def parseDepartments(file: RDD[String]): RDD[Department] = {
    cleanAndSplit(file).map { array => Department.fromArray(array) }
  }
  
  def cleanAndSplit(rdd: RDD[String]) = {
    rdd.map(_.trim.replaceAll("(INSERT INTO `.*` VALUES\\s*)|\\(|'|\\),|\\)|;", "")).filter(!_.isEmpty).map(_.split(","))
  }
  
}
