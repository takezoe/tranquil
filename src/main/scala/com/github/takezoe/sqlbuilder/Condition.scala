package com.github.takezoe.sqlbuilder

// TODO Don't hold raw SQL
case class Condition(sql: String) extends Sqlizable {

  def && (condition: Condition): Condition = Condition(s"(${sql} AND ${condition.sql})")

  def || (condition: Condition): Condition = Condition(s"(${sql} OR ${condition.sql})")

}

