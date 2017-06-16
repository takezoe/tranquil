package com.github.takezoe.tranquil

import java.sql.ResultSet

trait TableDef[R]{
  val tableName: String
  val columns: Seq[ColumnBase[_, _]]
  val alias: Option[String]
  def toModel(rs: ResultSet): R
  def wrap(alias: String): this.type
}

trait TableShape[T] {
  val definitions: T
  val columns: Seq[ColumnBase[_, _]]
  def wrap(alias: String): TableShape[T]
}

abstract class TableShapeBase[T](table: T) extends TableShape[T]{
  override val definitions: T = table
}

trait TableShapeOf[T] {
  def apply(table: T): TableShape[T]
}
