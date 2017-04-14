package com.github.takezoe.tranquil

// TODO Don't hold raw SQL
case class Sort(sql: String) {
  def ~(sort: Sort): Sort = Sort(s"${sql}, ${sort.sql}")
}
