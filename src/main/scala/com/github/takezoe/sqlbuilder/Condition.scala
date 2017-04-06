package com.github.takezoe.sqlbuilder

import java.sql.PreparedStatement

// TODO Don't hold raw SQL
case class Condition(sql: String, parameters: Seq[Param[_]] = Nil){

  def && (condition: Condition): Condition =
    Condition(s"(${sql} AND ${condition.sql})", parameters ++ condition.parameters)

  def || (condition: Condition): Condition =
    Condition(s"(${sql} OR ${condition.sql})",  parameters ++ condition.parameters)

}

case class Param[T](value: T, binder: Binder[T]){
  def set(stmt: PreparedStatement, i: Int): Unit = binder.set(value, stmt, i)
}