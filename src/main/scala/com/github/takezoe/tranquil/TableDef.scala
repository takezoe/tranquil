package com.github.takezoe.tranquil

import java.sql.ResultSet

trait TableDef[R]{
  val tableName: String

  lazy val columns: Seq[ColumnBase[_, _]] = {
    getClass.getDeclaredFields.filter { field =>
      classOf[ColumnBase[_, _]].isAssignableFrom(field.getType)
    }.map { field =>
      field.setAccessible(true)
      field.get(this).asInstanceOf[ColumnBase[_, _]]
    }.toSeq
  }

  def wrap(alias: String): this.type = {
    val constructor = getClass.getDeclaredConstructor(classOf[Option[String]])
    constructor.newInstance(Some(alias)).asInstanceOf[this.type]
  }

  val alias: Option[String]
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
