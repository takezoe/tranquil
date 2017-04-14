package com.github.takezoe.tranquil

import java.sql.ResultSet

trait TableDef[R] {
  val tableName: String
  val columns: Seq[Column[_]]
  val alias: Option[String]
  def toModel(rs: ResultSet): R
}


