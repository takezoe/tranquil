package com.github.takezoe.sqlbuilder

import scala.collection.mutable.ListBuffer

class Query[B <: TableDef, T](
  private val base: B,
  private val definitions: T,
  private val filters: Seq[Condition] = Nil,
  private val sorts: Seq[Sort] = Nil,
  private val innerJoins: Seq[(Query[_, _], Condition)] = Nil,
  private val leftJoins: Seq[(Query[_, _], Condition)] = Nil,
  private val columns: Seq[Column[_]] = Nil
) {

  private def isTableQuery: Boolean = {
    filters.isEmpty && sorts.isEmpty && innerJoins.isEmpty && leftJoins.isEmpty
  }

  private def getBase: TableDef = base

  def innerJoin[J <: TableDef](table: Query[J, J])(on: (T, J) => Condition): Query[B, (T, J)] = {
    new Query[B, (T, J)](
      base        = base,
      definitions = (definitions, table.base),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins :+ (table, on(definitions, table.base)),
      leftJoins   = leftJoins,
      columns     = columns
    )
  }

  def leftJoin[J <: TableDef](table: Query[J, J])(on: (T, J) => Condition): Query[B, (T, J)] = {
    new Query[B, (T, J)](
      base        = base,
      definitions = (definitions, table.base),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (table, on(definitions, table.base)),
      columns     = columns
    )
  }

  def filter(condition: T => Condition): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      filters     = filters :+ condition(definitions),
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      columns     = columns
    )
  }

  def sortBy(orderBy: T => Sort): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      filters     = filters,
      sorts       = sorts :+ orderBy(definitions),
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      columns     = columns
    )
  }

  def map(mapper: T => Columns): Query[B, T] = {
    new Query[B, T](
      base        = base,
      definitions = definitions,
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      columns     = mapper(definitions).columns
    )
  }

  def toSql(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("SELECT ")

    if(columns.nonEmpty){
      sb.append(columns.map { column =>
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
      if(query.isTableQuery){
        sb.append(query.getBase.tableName)
      } else {
        sb.append("(")
        sb.append(query.toSql(bindParams)._1)
        sb.append(")")
      }
      sb.append(" ")
      sb.append(query.getBase.alias)
      sb.append(" ON ")
      sb.append(condition.sql)
      bindParams ++= condition.parameters
    }

    leftJoins.foreach { case (query, condition) =>
      sb.append(" LEFT JOIN ")
      if(query.isTableQuery){
        sb.append(query.getBase.tableName)
      } else {
        sb.append("(")
        sb.append(query.toSql(bindParams))
        sb.append(")")
      }
      sb.append(" ")
      sb.append(query.getBase.alias)
      sb.append(" ON ")
      sb.append(condition.sql)
    }

    if(filters.nonEmpty){
      sb.append(" WHERE ")
      sb.append(filters.map(_.sql).mkString(" AND "))
      bindParams ++= filters.flatMap(_.parameters)
    }
    if(sorts.nonEmpty){
      sb.append(" ORDER BY ")
      sb.append(sorts.map(_.sql).mkString(", "))
    }

    (sb.toString(), bindParams)
  }
}
