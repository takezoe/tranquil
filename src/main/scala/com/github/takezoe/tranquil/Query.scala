package com.github.takezoe.tranquil

import java.sql.{Connection, ResultSet}

import scala.collection.mutable.ListBuffer

/**
 * Set of select columns and binder which retrieves values from these columns
 */
case class SelectColumns[T](columns: Seq[ColumnBase[_, _]], binder: ResultSet => T){

  def ~ [S](column: ColumnBase[_, S]): SelectColumns[(T, S)] = {
    SelectColumns(columns :+ column, (rs: ResultSet) => (binder(rs), column.get(rs)))
  }

  def get(rs: ResultSet): T = binder(rs)

}

/**
 * Define execution methods for Query.
 */
trait RunnableQuery[R] {
  protected val query: Query[_, _, R]

  // TODO It's possible to optimize the query for getting count.
  def count(conn: Connection): Int = {
    val (sql: String, bindParams: BindParams) = query.selectStatement(select = Some("COUNT(*) AS COUNT"))
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        rs.next
        rs.getInt("COUNT")
      }
    }
  }

  def list(conn: Connection): Seq[R] = {
    val (sql, bindParams) = query.selectStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        val list = new ListBuffer[R]
        while(rs.next){
          list += query.mapper(rs)
        }
        list.toSeq
      }
    }
  }

  def first(conn: Connection): R = {
    firstOption(conn).getOrElse {
      throw new NoSuchElementException()
    }
  }

  def firstOption(conn: Connection): Option[R] = {
    val (sql, bindParams) = query.selectStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()) { rs =>
        if (rs.next) {
          Some(query.mapper(rs))
        } else {
          None
        }
      }
    }
  }

}

/**
 * Represent a mapped query which is not accept additional operation except execution.
 */
class MappedQuery[R](protected val query: Query[_, _, R]) extends RunnableQuery[R] {
  def selectStatement(): (String, BindParams) = {
    query.selectStatement()
  }
}

/**
 *
 * @tparam B the base table definition type of this query
 * @tparam T the table type (with joined tables) of this query
 * @tparam R the result type of this query
 */
class Query[B <: TableDef[_], T, R](
  private val base: B,
  private val columns: Seq[ColumnBase[_, _]],
  private val definitions: T,
  private[tranquil] val mapper: ResultSet => R,
  private val filters: Seq[Condition] = Nil,
  private val sorts: Seq[Sort] = Nil,
  private val innerJoins: Seq[(Query[_, _, _], Condition)] = Nil,
  private val leftJoins: Seq[(Query[_, _, _], Condition)] = Nil,
  private val limit: Option[Int] = None,
  private val offset: Option[Int] = None
) extends RunnableQuery[R] {

  override protected val query = this

  private def isTableQuery: Boolean = {
    filters.isEmpty && sorts.isEmpty && innerJoins.isEmpty && leftJoins.isEmpty
  }

  private def getBase: TableDef[_] = base

  def map[J](f: T => SelectColumns[J]): MappedQuery[J] = {
    val select = f(definitions)

    new MappedQuery(new Query[B, T, J](
      base        = base,
      columns     = select.columns,
      definitions = definitions,
      mapper      = (rs: ResultSet) => select.get(rs),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    ))
  }

  def innerJoin[J <: TableDef[K], K](table: Query[J, J, K])(on: (T, J) => Condition): Query[B, (T, J), (R, K)] = {
    new Query[B, (T, J), (R, K)](
      base        = base,
      columns     = columns,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), table.base.toModel(rs)),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins :+ (table, on(definitions, table.base)),
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    )
  }

  def leftJoin[J <: TableDef[K], K](table: Query[J, J, K])(on: (T, J) => Condition): Query[B, (T, J), (R, Option[K])] = {
    new Query[B, (T, J), (R, Option[K])](
      base        = base,
      columns     = columns,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), if(rs.getObject(table.base.columns.head.asName) == null) None else Some(table.base.toModel(rs))),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (table, on(definitions, table.base)),
      limit       = limit,
      offset      = offset
    )
  }

  def filter(condition: T => Condition): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      columns     = columns,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters :+ condition(definitions),
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    )
  }

  def sortBy(orderBy: T => Sort): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      columns     = columns,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters,
      sorts       = sorts :+ orderBy(definitions),
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    )
  }

  def take(limit: Int): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      columns     = columns,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = Some(limit),
      offset      = offset
    )
  }

  def drop(offset: Int): Query[B, T, R] = {
    new Query[B, T, R](
      base        = base,
      columns     = columns,
      definitions = definitions,
      mapper      = mapper,
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = Some(offset)
    )
  }

  def selectStatement(bindParams: BindParams = new BindParams(), select: Option[String] = None): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("SELECT ")

    select match {
      case Some(x) => sb.append(x)
      case None => {
        val selectColumns = columns.map(c => c.fullName + " AS " + c.asName) ++
          innerJoins.flatMap { case (query, _) => query.getBase.columns.map(c => c.fullName + " AS " + c.asName) } ++
          leftJoins.flatMap  { case (query, _) => query.getBase.columns.map(c => c.fullName + " AS " + c.asName) }
        sb.append(selectColumns.mkString(", "))
      }
    }

    sb.append(" FROM ")
    sb.append(base.tableName)
    sb.append(" ")
    sb.append(base.alias.get)

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
      sb.append(query.getBase.alias.get)
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
      sb.append(query.getBase.alias.get)
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

    limit.foreach { limit =>
      sb.append(" LIMIT ").append(limit)
    }
    offset.foreach { offset =>
      sb.append(" OFFSET ").append(offset)
    }

    (sb.toString(), bindParams)
  }

}
