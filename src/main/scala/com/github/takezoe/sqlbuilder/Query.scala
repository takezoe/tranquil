package com.github.takezoe.sqlbuilder

import java.sql.{Connection, ResultSet}
import scala.collection.mutable.ListBuffer
import JDBCUtils._

class SingleTableQuery[B <: TableDef[_], R](
  private val base: B,
  private val mapper: ResultSet => R
) extends Query[B, B, R](base, base, mapper) {

  def insert(updateColumns: B => UpdateColumn): InsertAction = {
    new InsertAction(base, updateColumns(base))
  }

  def update(updateColumns: B => UpdateColumn)(filter: B => Condition): UpdateAction = {
    new UpdateAction(base, updateColumns(base), Seq(filter(base)))
  }

  def delete(filter: B => Condition): DeleteAction = {
    new DeleteAction(base, Seq(filter(base)))
  }

}

class InsertAction(tableDef: TableDef[_], updateColumn: UpdateColumn){

  def insertStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("INSERT INTO ")
    sb.append(tableDef.tableName)
    sb.append(" (")
    sb.append(updateColumn.columns.map(_.columnName).mkString(", "))
    sb.append(") VALUES (")
    sb.append(updateColumn.columns.map(_ => "?").mkString(", "))
    sb.append(")")
    bindParams ++= updateColumn.parameters

    (sb.toString, bindParams)
  }

  def execute(conn: Connection): Int = {
    val (sql, bindParams) = insertStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
    }
  }

}

class UpdateAction(tableDef: TableDef[_], updateColumn: UpdateColumn, filters: Seq[Condition]){

  def updateStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("UPDATE ")
    sb.append(tableDef.tableName)
    sb.append(" SET ")
    sb.append(updateColumn.columns.map { column => s"${column.columnName} = ?"}.mkString(", "))
    bindParams ++= updateColumn.parameters

    if(filters.nonEmpty){
      sb.append(" WHERE ")
      sb.append(filters.map(_.sql).mkString(" AND "))
      bindParams ++= filters.flatMap(_.parameters)
    }

    (sb.toString, bindParams)
  }

  def execute(conn: Connection): Int = {
    val (sql, bindParams) = updateStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
    }
  }

}

class DeleteAction(tableDef: TableDef[_], filters: Seq[Condition] = Nil){

  def deleteStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
    val sb = new StringBuilder()
    sb.append("DELETE FROM ")
    sb.append(tableDef.tableName)
    if(filters.nonEmpty){
      sb.append(" WHERE ")
      sb.append(filters.map(_.sql).mkString(" AND "))
      bindParams ++= filters.flatMap(_.parameters)
    }
    (sb.toString, bindParams)
  }

  def execute(conn: Connection): Int = {
    val (sql, bindParams) = deleteStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
    }
  }

}

//
//class UpdateQuery[B <: TableDef[_]](
//  private val base: B,
//  private val updateColumns: Seq[UpdateColumn],
//  private val filters: Seq[Condition] = Nil
//){
//
//  def filter(condition: B => Condition): UpdateQuery[B] = {
//    new UpdateQuery[B](
//      base          = base,
//      updateColumns = updateColumns,
//      filters       = filters :+ condition(base)
//    )
//  }
//
//  def set(f: (B) => UpdateColumn): UpdateQuery[B] = {
//    new UpdateQuery[B](base, updateColumns :+ f(base), filters)
//  }
//
//  def updateStatement(bindParams: BindParams = new BindParams()): (String, BindParams) = {
//    val sb = new StringBuilder()
//    sb.append("UPDATE ")
//    sb.append(base.tableName)
//    sb.append(" SET ")
//    sb.append(updateColumns.map(_.sql).mkString(", "))
//    bindParams ++= updateColumns.flatMap(_.parameters)
//
//    if(filters.nonEmpty){
//      sb.append(" WHERE ")
//      sb.append(filters.map(_.sql).mkString(" AND "))
//      bindParams ++= filters.flatMap(_.parameters)
//    }
//
//    (sb.toString, bindParams)
//  }
//
//  def update(conn: Connection): Int = {
//    val (sql, bindParams) = updateStatement()
//    using(conn.prepareStatement(sql)){ stmt =>
//      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
//      stmt.executeUpdate()
//    }
//  }
//
//}

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

}

object JDBCUtils {

  def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
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
