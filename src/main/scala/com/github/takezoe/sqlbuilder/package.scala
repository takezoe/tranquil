package com.github.takezoe

import java.sql.{JDBCType, PreparedStatement, ResultSet, Types}

import scala.language.implicitConversions

package object sqlbuilder {

//  implicit def column2columns[T](column: Column[T]): Columns[Column[T], T] = Columns[Column[T], T](column, Seq(column))

  object ~ {
    def unapply[A, B](t: (A, B)): Option[(A, B)] = Some(t)
  }

  implicit val stringBinder = new Binder[String] {
    override val jdbcType: Int = Types.VARCHAR
    override def set(value: String, stmt: PreparedStatement, i: Int): Unit = stmt.setString(i + 1, value)
    override def get(name: String, rs: ResultSet): String = rs.getString(name)
  }

  implicit val intBinder = new Binder[Int]{
    override val jdbcType: Int = Types.INTEGER
    override def set(value: Int, stmt: PreparedStatement, i: Int): Unit = stmt.setInt(i + 1, value)
    override def get(name: String, rs: ResultSet): Int = rs.getInt(name)
  }

  implicit val longBinder = new Binder[Long]{
    override val jdbcType: Int = Types.BIGINT
    override def set(value: Long, stmt: PreparedStatement, i: Int): Unit = stmt.setLong(i + 1, value)
    override def get(name: String, rs: ResultSet): Long = rs.getLong(name)
  }

  trait Binder[T] {
    val jdbcType: Int
    def set(value: T, stmt: PreparedStatement, i: Int): Unit
    def get(name: String, rs: ResultSet): T
  }

  private[sqlbuilder] def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
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

