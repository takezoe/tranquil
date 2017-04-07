package com.github.takezoe.sqlbuilder

import java.sql.{Connection, ResultSet}

import scala.collection.mutable.ListBuffer

class SingleTableQuery[B <: TableDef[_], T, R](
  private val base: B,
  private val definitions: T,
  private val mapper: ResultSet => R,
  private val filters: Seq[Condition] = Nil
) extends Query[B, T, R](base, definitions, mapper, filters) {

  override def filter(condition: T => Condition): SingleTableQuery[B, T, R] = {
    new SingleTableQuery[B, T, R](
      base        = base,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters :+ condition(definitions)
    )
  }

  def deleteStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("DELETE FROM ")
    sb.append(base.tableName)
    if(filters.nonEmpty){
      sb.append(" WHERE ")
      sb.append(filters.map(_.sql).mkString(" AND "))
      bindParams ++= filters.flatMap(_.parameters)
    }
    (sb.toString, bindParams)
  }

  def delete(conn: Connection): Int = {
    val (sql, bindParams) = deleteStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
    }
  }

}

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
      mapper      = (rs: ResultSet) => (mapper(rs), if(rs.getObject(table.base.columns.head.asName) == null) None else Some(table.base.toModel(rs))),
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

  def selectStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("SELECT ")

    val columns = base.columns.map(c => c.fullName + " AS " + c.asName) ++
      innerJoins.flatMap { case (query, _) => query.getBase.columns.map(c => c.fullName + " AS " + c.asName) } ++
      leftJoins.flatMap  { case (query, _) => query.getBase.columns.map(c => c.fullName + " AS " + c.asName) }
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
        sb.append(query.selectStatement(bindParams)._1)
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
        sb.append(query.selectStatement(bindParams))
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
    val (sql, bindParams) = selectStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        val list = new ListBuffer[R]
        while(rs.next){
          list += mapper(rs)
        }
        list.toSeq
      }
    }
  }

  def single(conn: Connection): Option[R] = {
    val (sql, bindParams) = selectStatement()
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

  protected def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
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
