package com.github.takezoe.tranquil

import java.sql.{PreparedStatement, ResultSet}

object Column {

  def apply[T](alias: Option[String], columnName: String, nullable: Boolean = false)(implicit binder: Binder[T]): Column[T] = {
    if(nullable){
      new NullableColumn[T](alias, columnName)
    } else {
      new Column[T](alias, columnName)
    }
  }

}

class Column[T](val alias: Option[String], val columnName: String)(implicit val binder: Binder[T]){

  val fullName = alias.map { x => x + "." + columnName } getOrElse columnName
  val asName   = alias.map { x => x + "_" + columnName } getOrElse columnName

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

  def asc: Sort = {
    Sort(s"${fullName} ASC")
  }

  def desc: Sort = {
    Sort(s"${fullName} DESC")
  }

  def -> (value: T)(implicit binder: Binder[T]): UpdateColumn = {
    UpdateColumn(Seq(this), Seq(Param(value, binder)))
  }

}

class NullableColumn[T](alias: Option[String], columnName: String)(implicit binder: Binder[T])
  extends Column[T](alias, columnName)(binder){

  def isNull(column: Column[T]): Condition = {
    Condition(s"${fullName} IS NULL")
  }

  def asNull: UpdateColumn = {
    UpdateColumn(Seq(this), Seq(Param(null, new Binder[Any]{
      override val jdbcType: Int = binder.jdbcType
      override def set(value: Any, stmt: PreparedStatement, i: Int): Unit = {
        stmt.setNull(i + 1, binder.jdbcType)
      }
      override def get(name: String, rs: ResultSet): T = ???
    })))
  }
}

//case class Columns[T, R](definitions: T, columns: Seq[Column[_]]){
//
//  def ~[U](column: Column[U]): Columns[(T, Column[U]), (T, U)] =
//    Columns[(T, Column[U]), (T, U)]((definitions, column), columns :+ column)
//
//  def toModel(rs: ResultSet): R = {
//    def _get(head: Column[_], rest: Seq[Column[_]]): Any = {
//      val result = head.get(rs)
//      rest match {
//        case Nil => result
//        case head :: rest => (result, _get(head, rest))
//      }
//    }
//    columns match {
//      case head :: rest => _get(head, rest).asInstanceOf[R]
//    }
//  }
//
//}

