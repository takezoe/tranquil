package com.github.takezoe.tranquil

import java.sql.ResultSet

trait TableDef[R] {
  val tableName: String
  val columns: Seq[ColumnBase[_, _]]
  val alias: Option[String]
  def toModel(rs: ResultSet): R
}


