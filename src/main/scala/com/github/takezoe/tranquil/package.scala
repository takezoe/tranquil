package com.github.takezoe

import java.sql._
import java.time._

import scala.language.implicitConversions

package object tranquil {

  // TODO Maybe timezone should be configurable?
  private val DefaultZone = ZoneId.systemDefault()
  private val DefaultOffset = DefaultZone.getRules().getOffset(Instant.now())

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

  implicit val booleanBinder = new Binder[Boolean]{
    override val jdbcType: Int = Types.BOOLEAN
    override def set(value: Boolean, stmt: PreparedStatement, i: Int): Unit = stmt.setBoolean(i + 1, value)
    override def get(name: String, rs: ResultSet): Boolean = rs.getBoolean(name)
  }

  implicit val dateBinder = new Binder[Date]{
    override val jdbcType: Int = Types.DATE
    override def set(value: java.sql.Date, stmt: PreparedStatement, i: Int): Unit = stmt.setDate(i + 1, value)
    override def get(name: String, rs: ResultSet): Date = rs.getDate(name)
  }

  implicit val timeBinder = new Binder[Time]{
    override val jdbcType: Int = Types.TIME
    override def set(value: Time, stmt: PreparedStatement, i: Int): Unit = stmt.setTime(i + 1, value)
    override def get(name: String, rs: ResultSet): Time = rs.getTime(name)
  }

  implicit val timestampBinder = new Binder[java.sql.Timestamp]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: Timestamp, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, value)
    override def get(name: String, rs: ResultSet): Timestamp = rs.getTimestamp(name)
  }

  implicit val utilDateBinder = new Binder[java.util.Date]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: java.util.Date, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.getTime))
    override def get(name: String, rs: ResultSet): java.util.Date = new Date(rs.getTimestamp(name).getTime)
  }

  implicit val localDateTimeBinder = new Binder[LocalDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: LocalDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp((value.toInstant(DefaultOffset).toEpochMilli)))
    override def get(name: String, rs: ResultSet): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(rs.getTimestamp(name).getTime), DefaultOffset)
  }

  implicit val zonedDateTimeBinder = new Binder[ZonedDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: ZonedDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.toInstant.toEpochMilli))
    override def get(name: String, rs: ResultSet): ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(rs.getTimestamp(name).getTime), DefaultZone)
  }

  implicit val offsetDateTimeBinder = new Binder[OffsetDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: OffsetDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.toInstant.toEpochMilli))
    override def get(name: String, rs: ResultSet): OffsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(rs.getTimestamp(name).getTime), DefaultZone)
  }


  trait Binder[T] {
    val jdbcType: Int
    def set(value: T, stmt: PreparedStatement, i: Int): Unit
    def get(name: String, rs: ResultSet): T
  }

  private[tranquil] def using[R <: AutoCloseable, T](resource: R)(f: R => T): T = {
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

