package com.github.takezoe.sqlbuilder

class Column[T](val alias: String, val columnName: String){

  def ==(column: Column[T]): Condition = {
    Condition(s"${alias}.${columnName} == ${column.alias}.${column.columnName}")
  }

  def ==(value: T): Condition = {
    Condition(s"${alias}.${columnName} == '${value}'") // TODO Placeholder
  }

  def !=(column: Column[T]): Condition = {
    Condition(s"${alias}.${columnName} <> ${column.alias}.${column.columnName}")
  }

  def !=(value: T): Condition = {
    Condition(s"${alias}.${columnName} <> '${value}'") // TODO Placeholder
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

}