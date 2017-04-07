package com.github.takezoe.sqlbuilder

import java.sql.ResultSet

class Column[T](val alias: String, val columnName: String)(implicit val binder: Binder[T]){

  val fullName = s"${alias}.${columnName}"
  val asName   = s"${alias}_${columnName}"

  def get(rs: ResultSet): T = {
    binder.get(asName, rs)
  }

  def getOpt(rs: ResultSet): Option[T] = {
    if(rs.getObject(asName) == null){
      None
    } else {
      Some(binder.get(asName, rs))
    }
  }


  def eq(column: Column[T]): Condition = {
    Condition(s"${fullName} = ${column.fullName}")
  }

  def eq(value: T)(implicit binder: Binder[T]): Condition = {
    Condition(s"${fullName} = ?", Seq(Param(value, binder)))
  }

  def ne(column: Column[T]): Condition = {
    Condition(s"${fullName} <> ${column.fullName}")
  }

  def ne(value: T)(implicit binder: Binder[T]): Condition = {
    Condition(s"${fullName} <> ?", Seq(Param(value, binder)))
  }

  def isNull(column: Column[T]): Condition = {
    Condition(s"${fullName} IS NULL")
  }

  def asc: Sort = {
    Sort(s"${fullName} ASC")
  }

  def desc: Sort = {
    Sort(s"${fullName} DESC")
  }

  def -> (value: T)(implicit binder: Binder[T]): UpdateColumn = {
    UpdateColumn(s"${fullName} = ?", Param(value, binder))
  }

//  def ~(column: Column[_]): Columns = Columns(Seq(this, column))

}

//case class Columns(columns: Seq[Column[_]]){
//
//  def ~(column: Column[_]): Columns = Columns(columns :+ column)
//
//}

