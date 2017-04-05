package com.github.takezoe.sqlbuilder

// TODO Don't hold raw SQL
case class Sort(sql: String) extends Sqlizable {
  def ~(sort: Sort): Sort = Sort(s"${sql}, ${sort.sql}")
}
