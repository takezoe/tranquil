package com.github.takezoe.tranquil

import java.sql.{PreparedStatement, ResultSet}

/**
 * Define basic functionality of the column model
 */
abstract sealed class ColumnBase[T, S](val tableDef: TableDef[_], val columnName: String, val auto: Boolean = false)
                               (implicit val binder: ColumnBinder[S]){

  val localName = tableDef.prefix.map { _ + columnName} getOrElse columnName
  val fullName  = tableDef.alias.map { x => x + "." + localName } getOrElse localName
  val aliasName = tableDef.alias.map { x => x + "_" + localName } getOrElse localName

  def wrap(alias: String): this.type

  def get(rs: ResultSet): S = {
    binder.get(aliasName, rs)
  }

  def startsWith(value: String): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "LIKE",
      Seq(Param(escape(value) + "%", binder.asInstanceOf[ColumnBinder[String]])))
  }

  def endsWith(value: String): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "LIKE",
      Seq(Param("%" + escape(value), binder.asInstanceOf[ColumnBinder[String]])))
  }

  def contains(value: String): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "LIKE",
      Seq(Param("%" + escape(value) + "%", binder.asInstanceOf[ColumnBinder[String]])))
  }

  private def escape(value: String): String = {
    value.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
  }

  def eq(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "=", Nil)
  }

  def eq(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "=", Seq(Param(value, binder)))
  }

  def eq(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "=", params.params)
  }

  def ne(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<>", Nil)
  }

  def ne(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<>", Seq(Param(value, binder)))
  }

  def ne(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<>", params.params)
  }

  def gt(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), ">", Nil)
  }

  def gt(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), ">", Seq(Param(value, binder)))
  }

  def gt(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), ">", params.params)
  }

  def ge(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), ">=", Nil)
  }

  def ge(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), ">=", Seq(Param(value, binder)))
  }

  def ge(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), ">=", params.params)
  }

  def lt(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<", Nil)
  }

  def lt(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<", Seq(Param(value, binder)))
  }

  def lt(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<", params.params)
  }

  def le(column: ColumnBase[T, _]): Condition = {
    Condition(SimpleColumnTerm(this), Some(SimpleColumnTerm(column)), "<=", Nil)
  }

  def le(value: T)(implicit binder: ColumnBinder[T]): Condition = {
    Condition(SimpleColumnTerm(this), Some(PlaceHolderTerm()), "<=", Seq(Param(value, binder)))
  }

  def le(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "<=", params.params)
  }

  def in(values: Seq[T])(implicit binder: ColumnBinder[T]): Condition = {
    val sql = values.map(_ => "?").mkString("(", ",", ")")
    val params = values.map(value => Param(value, binder))
    Condition(SimpleColumnTerm(this), Some(QueryTerm(sql)), "IN", params)
  }

  def in(query: RunnableQuery[_, T])(implicit dialect: Dialect): Condition = {
    val (sql, params) = query.selectStatement()
    Condition(SimpleColumnTerm(this), Some(QueryTerm("(" + sql + ")")), "IN", params.params)
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
    GroupingColumn(new FunctionColumn[Long](tableDef, columnName, s"COUNT(${fullName})", aliasName + "_COUNT"), false)
  }

  def max: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](tableDef, columnName, s"MAX(${fullName})", aliasName + "_MAX"), false)
  }

  def min: GroupingColumn[Long, Long] = {
    GroupingColumn(new FunctionColumn[Long](tableDef, columnName, s"MIN(${fullName})", aliasName + "_MIN"), false)
  }

  def avg: GroupingColumn[Double, Double] = {
    GroupingColumn(new FunctionColumn[Double](tableDef, columnName, s"AVG(${fullName})", aliasName + "_AVG"), false)
  }
}

/**
 * Represent a non-null column
 */
class Column[T](tableDef: TableDef[_], columnName: String, auto: Boolean = false)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, T](tableDef, columnName, auto)(binder){

  def toLowerCase: Column[T] = {
    new FunctionColumn[T](tableDef, columnName, s"LOWER(${fullName})", aliasName)
  }
  def toUpperCase: Column[T] = {
    new FunctionColumn[T](tableDef, columnName, s"UPPER(${fullName})", aliasName)
  }

  override def wrap(alias: String): Column.this.type = {
    new Column[T](tableDef.wrap(alias), columnName).asInstanceOf[this.type]
  }
}

///**
// * Represent an auto incremented column
// */
//class AutoIncrementColumn(alias: Option[String], columnName: String)
//  extends Column[Long](alias, columnName)(longBinder){
//
//  override def wrap(alias: String): AutoIncrementColumn.this.type = {
//    new AutoIncrementColumn(Some(alias), asName).asInstanceOf[this.type]
//  }
//}

/**
 * Represent a nullable column
 */
class OptionalColumn[T](tableDef: TableDef[_], columnName: String)(implicit binder: ColumnBinder[T])
  extends ColumnBase[T, Option[T]](tableDef, columnName)(new OptionalColumnBinder[T](binder)){

  def toLowerCase: OptionalColumn[T] = {
    new FunctionOptionalColumn[T](tableDef, columnName, s"LOWER(${fullName})", columnName)
  }
  def toUpperCase: OptionalColumn[T] = {
    new FunctionOptionalColumn[T](tableDef, columnName, s"UPPER(${fullName})", columnName)
  }

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

  override def wrap(alias: String): OptionalColumn.this.type = {
    new OptionalColumn[T](tableDef.wrap(alias), columnName).asInstanceOf[this.type]
  }
}

/**
 * Represent function call
 */
class FunctionColumn[T](tableDef: TableDef[_], columnName: String, select: String, name: String)
                       (implicit binder: ColumnBinder[T]) extends Column[T](tableDef, columnName)(binder){
  override val aliasName = name
  override val fullName  = select

  override def wrap(alias: String): FunctionColumn.this.type = {
    new FunctionColumn[T](tableDef.wrap(alias), "** columnName **", columnName, "** name **").asInstanceOf[this.type]
  }
}

/**
 * Represent function call for a nullable column
 */
class FunctionOptionalColumn[T](tableDef: TableDef[_], columnName: String, select: String, name: String)
                               (implicit binder: ColumnBinder[T])
  extends OptionalColumn[T](tableDef, columnName)(binder) {

  override val aliasName = name
  override val fullName  = select

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
