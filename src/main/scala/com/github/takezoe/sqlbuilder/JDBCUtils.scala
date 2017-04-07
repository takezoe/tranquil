package com.github.takezoe.sqlbuilder

object JDBCUtils {

  def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
    try {
      f(resource)
    } finally {
      if(resource != null){
        try {
          resource.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

}
