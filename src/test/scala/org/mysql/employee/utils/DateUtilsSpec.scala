package org.mysql.employee.utils

import org.scalatest.FunSpec
import org.scalatest.Matchers

class DateUtilsSpec extends FunSpec with Matchers{
  
  describe("toHumanTime") {

    it("can convert 1000ms to 1secs.") {
      DateUtils.toHumanTime(1000l) should equal("1secs.")
    }
    
    it("can convert 2000ms to 2secs."){
      DateUtils.toHumanTime(2000l) should equal("2secs.")
    }
    
    it("can convert 2999ms to 2secs."){
      DateUtils.toHumanTime(2999l) should equal("2secs.")
    }
    
    it("can convert 60000ms to 1min."){
      DateUtils.toHumanTime(60000l) should equal("1min.")
    }
    
    it("can convert 61000ms to 1min. 1secs."){
      DateUtils.toHumanTime(61000l) should equal("1min. 1secs.")
    }
    
    it("can convert 3600000ms to 1hours."){
      DateUtils.toHumanTime(3600000) should equal("1hrs.")
    }
    
    it("can convert 3660000ms to 1hours. 1min."){
      DateUtils.toHumanTime(3660000) should equal("1hrs. 1min.")
    }
    
    it("can convert 3601000ms to 1hours. 1secs."){
      DateUtils.toHumanTime(3601000) should equal("1hrs. 1secs.")
    }
    
    it("can convert 3661000ms to 1hours. 1min. 1secs."){
      DateUtils.toHumanTime(3661000) should equal("1hrs. 1min. 1secs.")
    }
    
    it("can convert 86400000ms to 1days"){
      DateUtils.toHumanTime(86400000) should equal("1days")
    }
    
    it("can convert 90000000ms to 1days 1hrs."){
      DateUtils.toHumanTime(90000000) should equal("1days 1hrs.")
    }
    
    it("can convert 86460000ms to 1days 1min."){
      DateUtils.toHumanTime(86460000) should equal("1days 1min.")
    }
    
    it("can convert 86401000ms to 1days 1secs."){
      DateUtils.toHumanTime(86401000) should equal("1days 1secs.")
    }
    
    it("can convert 90061000ms to 1days 1hrs. 1min. 1secs."){
      DateUtils.toHumanTime(90061000) should equal("1days 1hrs. 1min. 1secs.")
    }
    
  }
  
  
  
}