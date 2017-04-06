package com.github.takezoe.sqlbuilder

import java.sql.ResultSet

trait TableDef[R] {
  val tableName: String
  val columns: Seq[Column[_]]
  val alias: String
  def toModel(rs: ResultSet): R
}


