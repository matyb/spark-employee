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

object Main {

  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())

    validateArgs(logger, arg)

    val jobName = "Employee DB"

    val sc = createContext(jobName)

    val (pathToFiles, outputPath) = (arg(0), arg(1))

    rmFolder(outputPath)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    val employees = parseEmployees(sc.textFile(s"$pathToFiles/load_employees.dump"))

    employees.saveAsTextFile(outputPath)
  }

  def validateArgs(logger: Logger, arg: Array[String]) = {
    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files> <output-path>")
      System.exit(1)
    }
  }

  def createContext(jobName: String): SparkContext = {
    val conf = new SparkConf().setAppName(jobName).setMaster("local")
    new SparkContext(conf)
  }

  def parseEmployees(file: RDD[String]): RDD[Employee] = {
    val cleanRows = file.map(_.trim.replaceAll("INSERT INTO `[\\w]*` VALUES \\(","").replaceAll(",$","").replaceAll("'", ""))
    val employeeArrays = cleanRows.map(_.split(","))
    employeeArrays.foreach({x => println(x.mkString(","))})
    val result = employeeArrays.map { array => Employee.fromArray(array) }
    result
  }
  
}
