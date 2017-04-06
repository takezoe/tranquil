package com.github.takezoe.sqlbuilder

import java.sql.{Connection, PreparedStatement, ResultSet}

class Query[B <: TableDef[_], T, R](
  private val base: B,
  private val definitions: T,
  private val mapper: ResultSet => R,
  private val filters: Seq[Condition] = Nil,
  private val sorts: Seq[Sort] = Nil,
  private val innerJoins: Seq[(Query[_, _, _], Condition)] = Nil,
  private val leftJoins: Seq[(Query[_, _, _], Condition)] = Nil
) {

  private def isTableQuery: Boolean = {
    filters.isEmpty && sorts.isEmpty && innerJoins.isEmpty && leftJoins.isEmpty
  }

  private def getBase: TableDef[_] = base

  def innerJoin[J <: TableDef[K], K](table: Query[J, J, K])(on: (T, J) => Condition): Query[B, (T, J), (R, K)] = {
    new Query[B, (T, J), (R, K)](
      base        = base,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), table.base.toModel(rs)),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins :+ (table, on(definitions, table.base)),
      leftJoins   = leftJoins
    )
  }

  def leftJoin[J <: TableDef[K], K](table: Query[J, J, K])(on: (T, J) => Condition): Query[B, (T, J), (R, Option[K])] = {
    new Query[B, (T, J), (R, Option[K])](
      base        = base,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), if(table.base.columns.head.get(rs) == null) None else Some(table.base.toModel(rs))),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (table, on(definitions, table.base))
    )
  }

  def filter(condition: T => Condition): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters :+ condition(definitions),
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins
    )
  }

  def sortBy(orderBy: T => Sort): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters,
      sorts       = sorts :+ orderBy(definitions),
      innerJoins  = innerJoins,
      leftJoins   = leftJoins
    )
  }

  def toSql(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("SELECT ")

    sb.append(base.columns.map(_.fullName).mkString(", "))

    val columns = base.columns.map(_.fullName) ++
      innerJoins.flatMap { case (query, _) => query.getBase.columns.map(_.fullName) } ++
      leftJoins.flatMap  { case (query, _) => query.getBase.columns.map(_.fullName) }
    sb.append(columns.mkString(", "))

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

  def list(conn: Connection): Seq[R] = {
    val (sql, bindParams) = toSql()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        Iterator.continually(mapper(rs)).takeWhile(_ => rs.next).toSeq
      }
    }
  }

  def single(conn: Connection): Option[R] = {
    val (sql, bindParams) = toSql()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()) { rs =>
        if (rs.next) {
          Some(mapper(rs))
        } else {
          None
        }
      }
    }
  }

  private def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
    try {
      f(resource)
    } finally {
      if(resource != null){
        try {
          resource.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

}
