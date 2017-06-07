package com.github.takezoe.tranquil

import java.sql.{PreparedStatement, ResultSet}

/**
 * Define basic functionality of the column model
 */
abstract class ColumnBase[T, S](val alias: Option[String], val columnName: String)(implicit val binder: ColumnBinder[S]){

  val fullName = alias.map { x => x + "." + columnName } getOrElse columnName
  val asName   = alias.map { x => x + "_" + columnName } getOrElse columnName

  def get(rs: ResultSet): S = {
    binder.get(asName, rs)
  }

  def eq(column: ColumnBase[T, _]): Condition = {
    Condition(s"${fullName} = ${column.fullName}")
  }

  def eq(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(s"${fullName} = ?", Seq(Param(value, binder)))
  }

  def ne(column: ColumnBase[T, _]): Condition = {
    Condition(s"${fullName} <> ${column.fullName}")
  }

  def ne(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(s"${fullName} <> ?", Seq(Param(value, binder)))
  }

  def asc: Sort = {
    Sort(s"${fullName} ASC")
  }

  def desc: Sort = {
    Sort(s"${fullName} DESC")
  }

  def -> (value: T)(implicit binder: ColumnBinder[T]): UpdateColumn = {
    UpdateColumn(Seq(this), Seq(Param(value, binder)))
  }

}

/**
 * Represent a non-null column
 */
class Column[T](alias: Option[String], columnName: String)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, T](alias, columnName)(binder)


/**
 * Represent a nullable column
 */
class OptionalColumn[T](alias: Option[String], columnName: String)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, Option[T]](alias, columnName)(new OptionalColumnBinder[T](binder)){

  def isNull(column: ColumnBase[T, _]): Condition = {
    Condition(s"${fullName} IS NULL")
  }

  def asNull: UpdateColumn = {
    UpdateColumn(Seq(this), Seq(Param(null, new ColumnBinder[Any]{
      override val jdbcType: Int = binder.jdbcType
      override def set(value: Any, stmt: PreparedStatement, i: Int): Unit = {
        stmt.setNull(i + 1, binder.jdbcType)
      }
      override def get(name: String, rs: ResultSet): T = ???
    })))
  }
}

/**
 * A [[ColumnBinder]] implementation for [[OptionalColumn]] by wrapping a primitive binder.
 */
private class OptionalColumnBinder[T](binder: ColumnBinder[T]) extends ColumnBinder[Option[T]] {
  override val jdbcType: Int = binder.jdbcType

  override def set(value: Option[T], stmt: PreparedStatement, i: Int): Unit = {
    value match {
      case Some(x) => binder.set(x, stmt, i + 1)
      case None => stmt.setNull(i + 1, binder.jdbcType)
    }
  }

  override def get(name: String, rs: ResultSet): Option[T] = {
    if(rs.getObject(name) == null){
      None
    } else {
      Some(binder.get(name, rs))
    }
  }
}
