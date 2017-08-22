package com.github.takezoe.tranquil

import java.sql.{Connection, Statement}

class SingleTableAction[B <: TableDef[R], R <: Product](base: B){

  def insert(updateColumns: B => UpdateColumn): InsertAction[B] = {
    new InsertAction(base, updateColumns(base))
  }

  def insert(model: R): InsertAction[B] = {
    val columns = base.columns.zip(base.fromModel(model)).collect {
      case (column, value) if !column.auto => (column, Param(value, column.binder.asInstanceOf[ColumnBinder[Any]]))
    }
    new InsertAction(base, UpdateColumn(columns.map(_._1), columns.map(_._2)))
  }

  def update(updateColumns: B => UpdateColumn): UpdateAction[B] = {
    new UpdateAction(base, updateColumns(base))
  }

  def delete(): DeleteAction[B] = {
    new DeleteAction(base)
  }

}

class InsertAction[T <: TableDef[_]](tableDef: T, updateColumn: UpdateColumn){

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

  def executeAndReturnGeneratedId(conn: Connection): Long = {
    val (sql, bindParams) = insertStatement()
    using(conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
      using(stmt.getGeneratedKeys()){ rs =>
        rs.next()
        rs.getLong(1)
      }
    }
  }

  def execute(conn: Connection): Int = {
    val (sql, bindParams) = insertStatement()
    using(conn.prepareStatement(sql)){ stmt =>
      bindParams.params.zipWithIndex.foreach { case (param, i) => param.set(stmt, i) }
      stmt.executeUpdate()
    }
  }

}

class UpdateAction[T <: TableDef[_]](tableDef: T, updateColumn: UpdateColumn, filters: Seq[Condition] = Nil){

  def filter(filter: T => Condition): UpdateAction[T] = {
    new UpdateAction[T](tableDef, updateColumn, filters :+ filter(tableDef))
  }

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

class DeleteAction[T <: TableDef[_]](tableDef: T, filters: Seq[Condition] = Nil){

  def filter(filter: T => Condition): DeleteAction[T] = {
    new DeleteAction[T](tableDef, filters :+ filter(tableDef))
  }

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