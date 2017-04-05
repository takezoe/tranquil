package com.github.takezoe.sqlbuilder

class Query[B <: TableDefinition, T](
  private val base: B,
  private val definitions: T,
  private val wheres: Seq[Condition] = Nil,
  private val orderBys: Seq[OrderBy] = Nil,
  private val innerJoins: Seq[(Query[_, _], Condition)] = Nil,
  private val leftJoins: Seq[(Query[_, _], Condition)] = Nil,
  private val selects: Seq[Column[_]] = Nil
) extends Sqlizable {

  private def isTableQuery: Boolean = {
    wheres.isEmpty && orderBys.isEmpty && innerJoins.isEmpty
  }

  private def getBase: TableDefinition = base

  def innerJoin[J <: TableDefinition](table: Query[J, J])(on: (T, J) => Condition): Query[B, (T, J)] = {
    new Query[B, (T, J)](
      base  = this.base,
      definitions = (definitions, table.base),
      wheres      = wheres,
      orderBys    = orderBys,
      innerJoins  = innerJoins :+ (table, on(definitions, table.base)),
      leftJoins   = leftJoins,
      selects     = selects
    )
  }

  def leftJoin[J <: TableDefinition](table: Query[J, J])(on: (T, J) => Condition): Query[B, (T, J)] = {
    new Query[B, (T, J)](
      base  = this.base,
      definitions = (definitions, table.base),
      wheres      = wheres,
      orderBys    = orderBys,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (table, on(definitions, table.base)),
      selects     = selects
    )
  }

  def filter(condition: T => Condition): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      wheres      = wheres :+ condition(definitions),
      orderBys    = orderBys,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      selects     = selects
    )
  }

  def sortBy(orderBy: T => OrderBy): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      wheres      = wheres,
      orderBys    = orderBys :+ orderBy(definitions),
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      selects     = selects
    )
  }

  def map(mapper: T => Seq[Column[_]]): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      wheres      = wheres,
      orderBys    = orderBys,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      selects     = mapper(definitions)
    )
  }

  lazy val sql: String = {
    val sb = new StringBuilder()
    sb.append("SELECT ")

    if(selects.nonEmpty){
      sb.append(selects.map { column =>
        s"${column.alias}.${column.columnName}"
      }.mkString(", "))
    } else {
      // TODO Includes columns of joined tables?
      sb.append(base.columns.map { column =>
        s"${column.alias}.${column.columnName}"
      }.mkString(", "))
    }

    sb.append(" FROM ")
    sb.append(base.tableName)
    sb.append(" ")
    sb.append(base.alias)

    innerJoins.foreach { case (query, condition) =>
      sb.append(" INNER JOIN ")
      if(isTableQuery){
        sb.append(query.getBase.tableName)
      } else {
        sb.append("(")
        sb.append(query.sql)
        sb.append(")")
      }
      sb.append(" ")
      sb.append(query.getBase.alias)
      sb.append(" ON ")
      sb.append(condition.sql)
    }

    leftJoins.foreach { case (query, condition) =>
      sb.append(" LEFT JOIN ")
      if(isTableQuery){
        sb.append(query.getBase.tableName)
      } else {
        sb.append("(")
        sb.append(query.sql)
        sb.append(")")
      }
      sb.append(" ")
      sb.append(query.getBase.alias)
      sb.append(" ON ")
      sb.append(condition.sql)
    }

    if(wheres.nonEmpty){
      sb.append(" WHERE ")
      sb.append(wheres.map(_.sql).mkString(" AND "))
    }
    if(orderBys.nonEmpty){
      sb.append(" ORDER BY ")
      sb.append(orderBys.map(_.sql).mkString(", "))
    }
    sb.toString()
  }
}
