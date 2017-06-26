package com.github.takezoe.tranquil

trait Dialect {

  def in[T](column: ColumnBase[T, _], values: Seq[T])(implicit binder: ColumnBinder[T]): Condition = {
    val sql = values.map(_ => "?").mkString("(", ",", ")")
    val params = values.map(value => Param(value, binder))
    Condition(SimpleColumnTerm(column), Some(QueryTerm(sql)), "IN", params)
  }

  def paginate(sql: String, limit: Option[Int], offset: Option[Int]): String = {
    if(limit.isEmpty && offset.isEmpty){
      sql
    } else {
      val sb = new StringBuilder(sql)
      limit.foreach { limit =>
        sb.append(" LIMIT ").append(limit)
      }
      offset.foreach { offset =>
        sb.append(" OFFSET ").append(offset)
      }
      sb.toString()
    }
  }

}

class GenericDialect extends Dialect

class MySQLDialect extends Dialect {

  override def paginate(sql: String, limit: Option[Int], offset: Option[Int]): String = {
    (limit, offset) match {
      case (Some(limit), Some(offset)) => sql + " LIMIT " + offset + ", " + limit
      case (Some(limit), None)         => sql + " LIMIT " + limit
      case _                           => sql
    }
  }

}