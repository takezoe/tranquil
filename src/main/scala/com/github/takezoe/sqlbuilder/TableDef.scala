package com.github.takezoe.sqlbuilder

import java.sql.ResultSet

trait TableDef[R] {
  val tableName: String
  val columns: Seq[Column[_]]
  val alias: String
  def toModel(rs: ResultSet): R

  protected def get[T](rs: ResultSet, c: Column[T]): T = {
    c.binder.get(c.alias + "." + c.columnName, rs)
  }
}


