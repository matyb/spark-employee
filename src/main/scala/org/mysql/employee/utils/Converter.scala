package org.mysql.employee.utils

trait Converter[F, T] {
  def apply(from: F): T
}
