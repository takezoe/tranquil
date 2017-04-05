package com.github.takezoe.sqlbuilder

trait TableDef {
  val tableName: String
  val columns: Seq[Column[_]]
  val alias: String
}
