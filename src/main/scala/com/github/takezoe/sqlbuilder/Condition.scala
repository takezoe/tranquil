package com.github.takezoe.sqlbuilder

import java.sql.PreparedStatement

// TODO Don't hold raw SQL
/**
 * Filter condition in select statement
 */
case class Condition(sql: String, parameters: Seq[Param[_]] = Nil){

  def && (condition: Condition): Condition =
    Condition(s"(${sql} AND ${condition.sql})", parameters ++ condition.parameters)

  def || (condition: Condition): Condition =
    Condition(s"(${sql} OR ${condition.sql})",  parameters ++ condition.parameters)

}

// TODO Don't hold raw SQL
/**
 * Updating parameters in update statement
 */
case class UpdateColumn(sql: String, parameter: Param[_])

case class Param[T](value: T, binder: Binder[T]){
  def set(stmt: PreparedStatement, i: Int): Unit = binder.set(value, stmt, i)
}