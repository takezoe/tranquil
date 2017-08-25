package com.github.takezoe.tranquil

import java.sql.{Connection, ResultSet}

import scala.collection.mutable.ListBuffer

/**
 * Define execution methods for Query.
 */
trait RunnableQuery[T, R] {

  protected[tranquil] val underlying: Query[_ <: TableDef[_], T, R]
  protected[tranquil] val definitions: T

  def count(conn: Connection)(implicit dialect: Dialect): Int = {
    val (sql: String, bindParams: BindParams) = underlying._selectStatement(select = Some("COUNT(*) AS COUNT"))
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        rs.next
        rs.getInt("COUNT")
      }
    }
  }

  def list(conn: Connection)(implicit dialect: Dialect): Seq[R] = {
    val (sql, bindParams) = underlying.selectStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()){ rs =>
        val list = new ListBuffer[R]
        while(rs.next){
          list += underlying.mapper(rs)
        }
        list.toSeq
      }
    }
  }

  def first(conn: Connection)(implicit dialect: Dialect): R = {
    firstOption(conn).getOrElse {
      throw new NoSuchElementException()
    }
  }

  def firstOption(conn: Connection)(implicit dialect: Dialect): Option[R] = {
    val (sql, bindParams) = underlying.selectStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      using(stmt.executeQuery()) { rs =>
        if (rs.next) {
          Some(underlying.mapper(rs))
        } else {
          None
        }
      }
    }
  }

  def selectStatement()(implicit dialect: Dialect): (String, BindParams)

}

/**
 * Represent a mapped query which is not accept additional operation except execution.
 */
class MappedQuery[T, R](
  protected[tranquil] val underlying: Query[_ <: TableDef[_], T, R]
) extends RunnableQuery[T, R] {

  override protected[tranquil] val definitions: T = underlying.definitions
  override def selectStatement()(implicit dialect: Dialect): (String, BindParams) = underlying.selectStatement()
}

class GroupingQuery[T, R](
  protected[tranquil] val underlying: Query[_ <: TableDef[_], T, R],
  protected[tranquil] val definitions: T
) extends RunnableQuery[T, R] {

  override def selectStatement()(implicit dialect: Dialect): (String, BindParams) = underlying.selectStatement()

  def filter(condition: T => Condition): GroupingQuery[T, R] = {
    new GroupingQuery(
      underlying  = underlying.groupBy { _ =>
        val grouping = underlying.grouping.get.asInstanceOf[GroupingColumns[T, R]]
        grouping.copy(having = grouping.having :+ condition(definitions))
      }.underlying,
      definitions = definitions
    )
  }
}

class WrappedQuery[T, R](
  private[tranquil] val alias: String,
  private[tranquil] val query: RunnableQuery[T, R]
)(implicit val shape: TableShape[T]) extends RunnableQuery[T, R] {

  override def selectStatement()(implicit dialect: Dialect): (String, BindParams) = query.selectStatement()
  override protected[tranquil] val underlying: Query[_ <: TableDef[_], T, R] = query.underlying
  override protected[tranquil] val definitions: T = shape.wrap(alias).definitions
}

class SingleTableQuery[T <: TableDef[_], R](base: T) extends Query[T, T, R](base)

/**
 *
 * @tparam B the base table definition type of this query
 * @tparam T the table type (with joined tables) of this query
 * @tparam R the result type of this query
 */
class Query[B <: TableDef[_], T, R](
  protected[tranquil] val base: B,
  protected[tranquil] val columns: Seq[ColumnBase[_, _]],
  protected[tranquil] val definitions: T,
  protected[tranquil] val mapper: ResultSet => R,
  protected[tranquil] val filters: Seq[Condition] = Nil,
  protected[tranquil] val sorts: Seq[Sort] = Nil,
  protected[tranquil] val innerJoins: Seq[(RunnableQuery[_, _], String, Condition)] = Nil,
  protected[tranquil] val leftJoins: Seq[(RunnableQuery[_, _], String, Condition)] = Nil,
  protected[tranquil] val limit: Option[Int] = None,
  protected[tranquil] val offset: Option[Int] = None,
  protected[tranquil] val grouping: Option[GroupingColumns[_, R]] = None,
  protected[tranquil] val alias: Option[String] = None
) extends RunnableQuery[T, R] {

  def this(base: B) = this(
    base        = base,
    columns     = base.columns,
    definitions = base.asInstanceOf[T],
    mapper      = (base.toModel _).asInstanceOf[ResultSet => R]
  )

  override protected[tranquil] val underlying = this

  private def isTableQuery: Boolean = {
    filters.isEmpty && sorts.isEmpty && innerJoins.isEmpty && leftJoins.isEmpty && grouping.isEmpty
  }

  private def getBase: TableDef[_] = base

  def groupBy[T2, R2](f: T => GroupingColumns[T2, R2]): GroupingQuery[T2, R2] = {
    val grouping = f(definitions)

    new GroupingQuery[T2, R2](
      underlying = new Query[B, T2, R2](
        base        = base,
        columns     = grouping.columns.map(_.column),
        definitions = grouping.definition,
        mapper      = (rs: ResultSet) => grouping.get(rs),
        filters     = filters,
        sorts       = sorts,
        innerJoins  = innerJoins,
        leftJoins   = leftJoins,
        limit       = limit,
        offset      = offset,
        grouping    = Some(grouping)
      ),
      definitions = grouping.definition
    )
  }

  def map[T2, R2](f: T => SelectColumns[T2, R2]): MappedQuery[T2, R2] = {
    val select = f(definitions)

    new MappedQuery(new Query[B, T2, R2](
      base        = base,
      columns     = select.columns,
      definitions = select.definition,
      mapper      = (rs: ResultSet) => select.get(rs),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    ))
  }

  def innerJoin[T2 <: TableDef[R2], R2 <: Product](table: SingleTableQuery[T2, R2])(on: (T, T2) => Condition): Query[B, (T, T2), (R, R2)] = {
    new Query[B, (T, T2), (R, R2)](
      base        = base,
      columns     = columns,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), table.base.toModel(rs)),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins :+ (table, table.alias.getOrElse(table.base.alias.get), on(definitions, table.base)),
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    )
  }

  def innerJoin[T2, R2](query: RunnableQuery[T2, R2])(on: (T, T2) => Condition)
                       (implicit shapeOf: TableShapeOf[T2]): Query[B, (T, T2), (R, R2)] = {

    val alias = AliasGenerator.generate()
    val wrappedQuery = new WrappedQuery[T2, R2](alias, query)(shapeOf.apply(query.definitions))

    new Query[B, (T, T2), (R, R2)](
      base        = base,
      columns     = columns,
      definitions = (definitions, wrappedQuery.definitions),
      mapper      = (rs: ResultSet) => (mapper(rs), wrappedQuery.underlying.mapper(rs)),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins :+ (wrappedQuery, alias, on(definitions, wrappedQuery.definitions)),
      leftJoins   = leftJoins,
      limit       = limit,
      offset      = offset
    )
  }

  def leftJoin[T2 <: TableDef[R2], R2 <: Product](table: SingleTableQuery[T2, R2])(on: (T, T2) => Condition): Query[B, (T, T2), (R, Option[R2])] = {
    new Query[B, (T, T2), (R, Option[R2])](
      base        = base,
      columns     = columns,
      definitions = (definitions, table.base),
      mapper      = (rs: ResultSet) => (mapper(rs), if(rs.getObject(table.base.columns.head.aliasName) == null) None else Some(table.base.toModel(rs))),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (table, table.alias.getOrElse(table.base.alias.get), on(definitions, table.base)),
      limit       = limit,
      offset      = offset
    )
  }

  def leftJoin[T2, R2](query: RunnableQuery[T2, R2])(on: (T, T2) => Condition)
                      (implicit shapeOf: TableShapeOf[T2]): Query[B, (T, T2), (R, Option[R2])] = {

    val alias = AliasGenerator.generate()
    val wrappedQuery = new WrappedQuery[T2, R2](alias, query)(shapeOf.apply(query.definitions))

    new Query[B, (T, T2), (R, Option[R2])](
      base        = base,
      columns     = columns,
      definitions = (definitions, wrappedQuery.definitions),
      mapper      = (rs: ResultSet) => (mapper(rs), if(rs.getObject(wrappedQuery.underlying.columns.head.aliasName) == null) None else Some(wrappedQuery.underlying.mapper(rs))),
      filters     = filters,
      sorts       = sorts,
      innerJoins  = innerJoins,
      leftJoins   = leftJoins :+ (wrappedQuery, alias, on(definitions, wrappedQuery.definitions)),
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

  override def selectStatement()(implicit dialect: Dialect): (String, BindParams) = {
    _selectStatement()
  }

  private[tranquil] def _selectStatement(
    bindParams: BindParams = new BindParams(),
    select: Option[String] = None
  )(implicit dialect: Dialect): (String, BindParams) = {

    val sb = new StringBuilder()
    sb.append("SELECT ")

    select match {
      case Some(x) => sb.append(x)
      case None => {
        val selectColumns = columns.map(c => c.fullName + " AS " + c.aliasName) ++
          innerJoins.flatMap { case (query, alias, _) =>
            if(query.underlying.isTableQuery){
              query.underlying.getBase.columns.map(c => c.fullName + " AS " + c.aliasName)
            } else {
              query.underlying.columns.map(c => alias + "." + c.aliasName + " AS " + c.aliasName)
            }
          } ++
          leftJoins.flatMap  { case (query, alias, _) =>
            if(query.underlying.isTableQuery){
              query.underlying.getBase.columns.map(c => c.fullName + " AS " + c.aliasName)
            } else {
              query.underlying.columns.map(c => alias + "." + c.aliasName + " AS " + c.aliasName)
            }
          }
        sb.append(selectColumns.mkString(", "))
      }
    }

    sb.append(" FROM ")
    sb.append(base.tableName)
    sb.append(" ")
    sb.append(base.alias.get)

    innerJoins.foreach { case (query, alias, condition) =>
      sb.append(" INNER JOIN ")
      if(query.underlying.isTableQuery){
        sb.append(query.underlying.getBase.tableName)
        sb.append(" ")
        sb.append(alias)
        sb.append(" ON ")
        sb.append(condition.sql)
        bindParams ++= condition.parameters
      } else {
        val (sql, bind) = query.selectStatement()
        sb.append("(")
        sb.append(sql)
        sb.append(") ")
        sb.append(alias)
        sb.append(" ON ")
        sb.append(condition.sql)
        bindParams ++= bind.params
        bindParams ++= condition.parameters
      }
    }

    leftJoins.foreach { case (query, alias, condition) =>
      sb.append(" LEFT JOIN ")
      if(query.underlying.isTableQuery){
        sb.append(query.underlying.getBase.tableName)
        sb.append(" ")
        sb.append(alias)
        sb.append(" ON ")
        sb.append(condition.sql)
      } else {
        sb.append("(")
        val (sql, bind) = query.selectStatement()
        sb.append(sql)
        sb.append(") ")
        sb.append(alias)
        sb.append(" ON ")
        sb.append(condition.sql)
        bindParams ++= bind.params
        bindParams ++= condition.parameters
      }
    }

    if(filters.nonEmpty){
      sb.append(" WHERE ")
      sb.append(filters.map(_.sql).mkString(" AND "))
      bindParams ++= filters.flatMap(_.parameters)
    }

    grouping.foreach { grouping =>
      sb.append(" GROUP BY ")
      sb.append(grouping.groupByColumns.map(_.column.fullName).mkString(", "))
      if(grouping.having.nonEmpty){
        sb.append(" HAVING ")
        sb.append(grouping.having.map(_.sql).mkString(" AND "))
        bindParams ++= grouping.having.flatMap(_.parameters)
      }
    }

    if(sorts.nonEmpty){
      sb.append(" ORDER BY ")
      sb.append(sorts.map(_.sql).mkString(", "))
    }

//    limit.foreach { limit =>
//      sb.append(" LIMIT ").append(limit)
//    }
//    offset.foreach { offset =>
//      sb.append(" OFFSET ").append(offset)
//    }

    (dialect.paginate(sb.toString, limit, offset), bindParams)
  }

}
