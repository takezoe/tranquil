package com.github.takezoe.tranquil

import java.sql.ResultSet

abstract class TableDef[R](val tableName: String) extends Product {
  lazy val columns: Seq[ColumnBase[_, _]] = {
    productIterator.collect { case c: ColumnBase[_, _] => c }.toSeq
  }
  val alias: Option[String]
  def wrap(alias: String): this.type
  def toModel(rs: ResultSet): R
}

abstract class TableShape[T](table: T){
  val definitions: T = table
  val columns: Seq[ColumnBase[_, _]]
  def wrap(alias: String): TableShape[T]
}

trait TableShapeOf[T] {
  def apply(table: T): TableShape[T]
}
