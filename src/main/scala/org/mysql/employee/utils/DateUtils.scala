package org.mysql.employee.utils

import org.mysql.employee.constants.DateConstants
import java.text.SimpleDateFormat

object DateUtils {
  
  def toHumanTime(ms: Long) = {
    var x = ms / 1000
    val seconds = x % 60; x /= 60;
    val minutes = x % 60; x /= 60;
    val hours = x % 24 
    val days = x / 24
    (s"${if (days > 0) days + "days " else ""}" +
      s"${if (hours > 0) hours + "hrs. " else ""}" +
      s"${if (minutes > 0) minutes + "min. " else ""}" +
      s"${if (seconds > 0) seconds + "secs. " else ""}").trim()
  }
  
  def outputFormat() = {
    new SimpleDateFormat(DateConstants.outputDateFormat)
  }
  
  def ingestionFormat() = {
    new SimpleDateFormat(DateConstants.ingestionDateFormat)
  }
  
  def outputTimeFormat() = {
    new SimpleDateFormat(DateConstants.outputDateFormat + " hh:mm:ss")
  }
  
}