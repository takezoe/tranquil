package com.github.takezoe.tranquil

import java.sql.PreparedStatement

// TODO Don't hold raw SQL
/**
 * Set of filter condition and its parameters in select statement
 */
case class Condition(sql: String, parameters: Seq[Param[_]] = Nil){

  def && (condition: Condition): Condition =
    Condition(s"(${sql} AND ${condition.sql})", parameters ++ condition.parameters)

  def || (condition: Condition): Condition =
    Condition(s"(${sql} OR ${condition.sql})",  parameters ++ condition.parameters)

}

// TODO Don't hold raw SQL
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