package com.github.takezoe.tranquil

import java.sql.{PreparedStatement, ResultSet}

/**
 * Define basic functionality of the column model
 */
abstract class ColumnBase[T, S](val alias: Option[String], val columnName: String)
                               (implicit val binder: ColumnBinder[S]){

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

  def count: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"COUNT(${fullName})"), false)
  }

  def max: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"MAX(${fullName})"), false)
  }

  def min: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"MIN(${fullName})"), false)
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
 * Represent function call
 */
class FunctionColumn[T](alias: Option[String], columnName: String, select: String)
                       (implicit binder: ColumnBinder[T]) extends Column[T](alias, columnName)(binder){
  override val fullName = select
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

/**
 * Set of select columns and binder which retrieves values from these columns
 */
case class SelectColumns[T](columns: Seq[ColumnBase[_, _]], binder: ResultSet => T){

  def ~ [S](column: ColumnBase[_, S]): SelectColumns[(T, S)] = {
    SelectColumns(columns :+ column, (rs: ResultSet) => (binder(rs), column.get(rs)))
  }

  def get(rs: ResultSet): T = binder(rs)

}

/**
 * Represent a columns in GROUP BY query.
 *
 * If groupBy is true, this column means used as a grouping key.
 * Otherwise, this column means aggregate function call.
 */
case class GroupingColumn[S, T](column: ColumnBase[S, T], groupBy: Boolean = true)

/**
 * Set of grouping columns.
 */
case class GroupingColumns[T, R](definition: T, columns: Seq[GroupingColumn[_, _]], binder: ResultSet => R){

  def ~ [S, K](column: GroupingColumn[K, S]): GroupingColumns[(T, ColumnBase[K, S]), (R, S)] = {
    GroupingColumns(
      (definition, column.column),
      columns :+ column,
      (rs: ResultSet) => (binder(rs), column.column.get(rs))
    )
  }

  def get(rs: ResultSet): R = binder(rs)

  lazy val groupByColumns = columns.filter(_.groupBy)

}
