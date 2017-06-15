package com.github.takezoe.tranquil

import java.sql.PreparedStatement

trait ConditionTerm {
  val underlying: Option[ColumnBase[_, _]]
  def sql: String
}

case class SimpleColumnTerm(column: ColumnBase[_, _]) extends ConditionTerm {
  override val underlying: Option[ColumnBase[_, _]] = Some(column)
  override def sql: String = column.fullName
}

case class PlaceHolderTerm() extends ConditionTerm {
  override val underlying: Option[ColumnBase[_, _]] = None
  override def sql: String = "?"
}

case class QueryTerm(query: String) extends ConditionTerm {
  override val underlying: Option[ColumnBase[_, _]] = None
  override def sql: String = query
}

/**
 * Set of filter condition and its parameters in select statement
 */
case class Condition(
  left: ConditionTerm,
  right: Option[ConditionTerm] = None,
  operator: String = "",
  parameters: Seq[Param[_]] = Nil
) extends ConditionTerm {
  override val underlying: Option[ColumnBase[_, _]] = None

  def && (condition: Condition): Condition = {
    Condition(this, Some(condition), "AND", parameters ++ condition.parameters)
  }

  def || (condition: Condition): Condition = {
    Condition(this, Some(condition), "OR", parameters ++ condition.parameters)
  }

  override def sql: String = {
    left.sql + right.map { term =>
      " " + operator + " " + term.sql
    }.getOrElse {
      if(operator.isEmpty) "" else " " + operator
    }
  }

}

/**
 * Set of columns and parameters to be updated in update statement
 */
case class UpdateColumn(columns: Seq[ColumnBase[_, _]], parameters: Seq[Param[_]]){

  def ~ (updateColumn: UpdateColumn): UpdateColumn = {
    UpdateColumn(columns ++ updateColumn.columns, parameters ++ updateColumn.parameters)
  }

}

case class Param[T](value: T, binder: ColumnBinder[T]){
  def set(stmt: PreparedStatement, i: Int): Unit = binder.set(value, stmt, i)
}