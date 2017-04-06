package com.github.takezoe.sqlbuilder

class Column[T](val alias: String, val columnName: String)(implicit val binder: Binder[T]){

  def ==(column: Column[T]): Condition = {
    Condition(s"${alias}.${columnName} == ${column.alias}.${column.columnName}")
  }

  def ==(value: T)(implicit binder: Binder[T]): Condition = {
    Condition(s"${alias}.${columnName} == ?", Seq(Param(value, binder)))
  }

  def !=(column: Column[T]): Condition = {
    Condition(s"${alias}.${columnName} <> ${column.alias}.${column.columnName}")
  }

  def !=(value: T)(implicit binder: Binder[T]): Condition = {
    Condition(s"${alias}.${columnName} <> ?", Seq(Param(value, binder)))
  }

  def isNull(column: Column[T]): Condition = {
    Condition(s"${alias}.${columnName} IS NULL")
  }

  def asc: Sort = {
    Sort(s"${alias}.${columnName} ASC")
  }

  def desc: Sort = {
    Sort(s"${alias}.${columnName} DESC")
  }

//  def ~(column: Column[_]): Columns = Columns(Seq(this, column))

}

//case class Columns(columns: Seq[Column[_]]){
//
//  def ~(column: Column[_]): Columns = Columns(columns :+ column)
//
//}

