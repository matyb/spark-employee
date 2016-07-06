package org.mysql.employee.utils

object Companion {

  def companion[R, T](m: Manifest[T]) = {
    val classOfT = m.runtimeClass
    Class.forName(classOfT.getName + "$").getField("MODULE$").get(null).asInstanceOf[R]
  }

}