package org.mysql.employee.constants

import java.text.SimpleDateFormat

object DateConstants {
  val ingestionDateFormat = "yyyy-MM-dd"
  val outputDateFormat = "MM/dd/yyyy"
  val endOfTime = new SimpleDateFormat(ingestionDateFormat).parse("9999-12-31")
}