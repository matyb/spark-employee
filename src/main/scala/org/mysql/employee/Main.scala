package org.mysql.employee

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mysql.employee.domain.Department
import org.mysql.employee.domain.DepartmentEmployee
import org.mysql.employee.domain.DepartmentManager
import org.mysql.employee.domain.Employee
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

    val employeeDemographics = parse(sc.textFile(s"$pathToFiles/load_employees.dump"), EmployeeDemographic).cache()
    val departments = parse(sc.textFile(s"$pathToFiles/load_departments.dump"), Department).cache()
    val departmentEmployees = parse(sc.textFile(s"$pathToFiles/load_dept_emp.dump"), DepartmentEmployee).cache()
    val departmentManagers = parse(sc.textFile(s"$pathToFiles/load_dept_manager.dump"), DepartmentManager).cache()
    val employeeTitles = parse(sc.textFile(s"$pathToFiles/load_titles.dump"), EmployeeTitle).cache()
    val employeeSalaries = parse(sc.textFile(s"$pathToFiles/load_salaries1.dump"), EmployeeSalary).union(
                           parse(sc.textFile(s"$pathToFiles/load_salaries2.dump"), EmployeeSalary).union(
                           parse(sc.textFile(s"$pathToFiles/load_salaries3.dump"), EmployeeSalary))).cache()                           

    val employees = join(departments, departmentEmployees, departmentManagers, 
                         employeeDemographics, employeeTitles, employeeSalaries).cache()                           
                           
    employeeDemographics.saveAsTextFile(s"$outputPath/employee_demographics")
    departments.saveAsTextFile(s"$outputPath/departments")
    departmentEmployees.saveAsTextFile(s"$outputPath/department_employees")
    departmentManagers.saveAsTextFile(s"$outputPath/department_managers")
    employeeTitles.saveAsTextFile(s"$outputPath/employee_titles")
    employeeSalaries.saveAsTextFile(s"$outputPath/employee_salaries")
    employees.saveAsTextFile(s"$outputPath/employees")
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

  def join(departments: RDD[Department], departmentEmployees: RDD[DepartmentEmployee], departmentManagers: RDD[DepartmentManager], employeeDemographics: RDD[EmployeeDemographic], employeeTitles: RDD[EmployeeTitle], employeeSalaries: RDD[EmployeeSalary]) = {
    val departmentsRdd = departments.map { row => (row.id, row) }
    val departmentEmployeesDepRdd = departmentsRdd.join(departmentEmployees.map { row => (row.departmentId, row) })
    val departmentEmployeesEmpRdd = departmentEmployeesDepRdd.map { row => (row._2._2.employeeId, row._2) }
    val departmentManagerDepRdd = departmentsRdd.join(departmentManagers.map { row => (row.managedDepartmentId, row) })
                                                .map{ row => (row._2._2.employeeId, (row._2._1, row._2._2)) }
    val employeeDemographicsRdd = employeeDemographics.map { row => (row.employeeId, row )}
                                                      .leftOuterJoin(departmentManagerDepRdd)
    
    println(s"departmentManagers:${departmentManagers.collect()}")
    println(s"departmentManagerDepRdd:${departmentManagerDepRdd.collect()}")
    
    val grouped = departmentEmployeesEmpRdd
                    .join(employeeDemographicsRdd
                        .join(employeeSalaries.map { row => (row.employeeId, row) })
                        .join(employeeTitles.map { row => (row.employeeId, row) } )).groupBy { row => row._1 }
    
    grouped.map { row =>
      val departmentEmployee = ListBuffer[(DepartmentEmployee, Department)]()
      val departmentManager = ListBuffer[(Department, DepartmentManager)]()
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
