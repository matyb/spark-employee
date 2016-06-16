package org.mysql.employee.utils

trait Converter[F,T] {
  
  def convert(from: F) : T
  
}