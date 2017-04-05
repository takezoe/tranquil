package com.github.takezoe.sqlbuilder

trait TableDefinition {
  val tableName: String
  val columns: Seq[Column[_]]
  val alias: String
}
