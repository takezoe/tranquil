package com.github.takezoe.tranquil

import java.sql.{PreparedStatement, ResultSet}

/**
 * Define basic functionality of the column model
 */
abstract class ColumnBase[T, S](val alias: Option[String], val columnName: String)
                               (implicit val binder: ColumnBinder[S]){

  val fullName = alias.map { x => x + "." + columnName } getOrElse columnName
  val asName   = alias.map { x => x + "_" + columnName } getOrElse columnName

  protected[tranquil] def lift(query: Query[_, _, _]): ColumnBase[T, S]

  def get(rs: ResultSet): S = {
    binder.get(asName, rs)
  }

  def eq(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "=", Nil)
  }

  def eq(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "=", Seq(Param(value, binder)))
  }

  def eq(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "=", params.params)
  }

  def ne(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<>", Nil)
  }

  def ne(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<>", Seq(Param(value, binder)))
  }

  def ne(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<>", params.params)
  }

  def gt(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), ">", Nil)
  }

  def gt(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), ">", Seq(Param(value, binder)))
  }

  def gt(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), ">", params.params)
  }

  def ge(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), ">=", Nil)
  }

  def ge(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), ">=", Seq(Param(value, binder)))
  }

  def ge(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), ">=", params.params)
  }

  def lt(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<", Nil)
  }

  def lt(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<", Seq(Param(value, binder)))
  }

  def lt(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<", params.params)
  }

  def le(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<=", Nil)
  }

  def le(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<+", Seq(Param(value, binder)))
  }

  def le(query: RunnableQuery[_, T]): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<=", params.params)
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
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"COUNT(${fullName})", asName + "_COUNT"), false)
  }

  def max: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"MAX(${fullName})", asName + "_MAX"), false)
  }

  def min: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](alias, columnName, s"MIN(${fullName})", asName + "_MIN"), false)
  }
}

/**
 * Represent a non-null column
 */
class Column[T](alias: Option[String], columnName: String)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, T](alias, columnName)(binder){

  override protected[tranquil] def lift(query: Query[_, _, _]) = {
    if(query.columns.contains(this)){
      new Column[T](query.alias, asName)
    } else this
  }
}


/**
 * Represent a nullable column
 */
class OptionalColumn[T](alias: Option[String], columnName: String)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, Option[T]](alias, columnName)(new OptionalColumnBinder[T](binder)){

  def isNull(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), None, "IS NULL")
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

  override protected[tranquil] def lift(query: Query[_, _, _]) = {
    if(query.columns.contains(this)){
      new OptionalColumn[T](query.alias, asName)
    } else this
  }
}

/**
 * Represent function call
 */
class FunctionColumn[T](alias: Option[String], columnName: String, select: String, name: String)
                       (implicit binder: ColumnBinder[T]) extends Column[T](alias, columnName)(binder){
  override val asName = name
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
case class SelectColumns[T, R](
  definition: T,
  columns: Seq[ColumnBase[_, _]],
  binder: ResultSet => R
){
  def ~ [S, K](column: ColumnBase[K, S]): SelectColumns[(T, ColumnBase[K, S]), (R, S)] = {
    SelectColumns(
      (definition, column),
      columns :+ column,
      (rs: ResultSet) => (binder(rs), column.get(rs))
    )
  }

  def get(rs: ResultSet): R = binder(rs)

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
case class GroupingColumns[T, R](
  definition: T,
  columns: Seq[GroupingColumn[_, _]],
  binder: ResultSet => R,
  having: Seq[Condition] = Nil
){
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
