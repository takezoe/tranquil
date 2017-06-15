package com.github.takezoe

import java.sql._
import java.time._

import scala.language.implicitConversions

package object tranquil {

  implicit val DefaultZone = ZoneId.systemDefault()

  object ~ {
    def unapply[A, B](t: (A, B)): Option[(A, B)] = Some(t)
  }

  implicit val stringBinder = new ColumnBinder[String] {
    override val jdbcType: Int = Types.VARCHAR
    override def set(value: String, stmt: PreparedStatement, i: Int): Unit = stmt.setString(i + 1, value)
    override def get(name: String, rs: ResultSet): String = rs.getString(name)
  }

  implicit val intBinder = new ColumnBinder[Int]{
    override val jdbcType: Int = Types.INTEGER
    override def set(value: Int, stmt: PreparedStatement, i: Int): Unit = stmt.setInt(i + 1, value)
    override def get(name: String, rs: ResultSet): Int = rs.getInt(name)
  }

  implicit val longBinder = new ColumnBinder[Long]{
    override val jdbcType: Int = Types.BIGINT
    override def set(value: Long, stmt: PreparedStatement, i: Int): Unit = stmt.setLong(i + 1, value)
    override def get(name: String, rs: ResultSet): Long = rs.getLong(name)
  }

  implicit val booleanBinder = new ColumnBinder[Boolean]{
    override val jdbcType: Int = Types.BOOLEAN
    override def set(value: Boolean, stmt: PreparedStatement, i: Int): Unit = stmt.setBoolean(i + 1, value)
    override def get(name: String, rs: ResultSet): Boolean = rs.getBoolean(name)
  }

  implicit val dateBinder = new ColumnBinder[Date]{
    override val jdbcType: Int = Types.DATE
    override def set(value: java.sql.Date, stmt: PreparedStatement, i: Int): Unit = stmt.setDate(i + 1, value)
    override def get(name: String, rs: ResultSet): Date = rs.getDate(name)
  }

  implicit val timeBinder = new ColumnBinder[Time]{
    override val jdbcType: Int = Types.TIME
    override def set(value: Time, stmt: PreparedStatement, i: Int): Unit = stmt.setTime(i + 1, value)
    override def get(name: String, rs: ResultSet): Time = rs.getTime(name)
  }

  implicit val timestampBinder = new ColumnBinder[java.sql.Timestamp]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: Timestamp, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, value)
    override def get(name: String, rs: ResultSet): Timestamp = rs.getTimestamp(name)
  }

  implicit val utilDateBinder = new ColumnBinder[java.util.Date]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: java.util.Date, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.getTime))
    override def get(name: String, rs: ResultSet): java.util.Date = new Date(rs.getTimestamp(name).getTime)
  }

  implicit def localDateTimeBinder(implicit timeZone: ZoneId) = new ColumnBinder[LocalDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: LocalDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, Timestamp.valueOf(value))
    override def get(name: String, rs: ResultSet): LocalDateTime = rs.getTimestamp(name).toInstant.atZone(timeZone).toLocalDateTime
  }

  implicit def localDateBinder(implicit timeZone: ZoneId) = new ColumnBinder[LocalDate]{
    override val jdbcType: Int = Types.DATE
    override def set(value: LocalDate, stmt: PreparedStatement, i: Int): Unit = stmt.setDate(i + 1, Date.valueOf(value))
    override def get(name: String, rs: ResultSet): LocalDate = rs.getDate(name).toInstant.atZone(timeZone).toLocalDate
  }

  implicit def localTimeBinder(implicit timeZone: ZoneId) = new ColumnBinder[LocalTime]{
    override val jdbcType: Int = Types.TIME
    override def set(value: LocalTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTime(i + 1, Time.valueOf(value))
    override def get(name: String, rs: ResultSet): LocalTime = rs.getTime(name).toInstant.atZone(timeZone).toLocalTime
  }

  implicit def zonedDateTimeBinder(implicit timeZone: ZoneId) = new ColumnBinder[ZonedDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: ZonedDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.toInstant.toEpochMilli))
    override def get(name: String, rs: ResultSet): ZonedDateTime = rs.getTimestamp(name).toInstant.atZone(timeZone)
  }

  implicit def offsetDateTimeBinder(implicit timeZone: ZoneId) = new ColumnBinder[OffsetDateTime]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: OffsetDateTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.toInstant.toEpochMilli))
    override def get(name: String, rs: ResultSet): OffsetDateTime = rs.getTimestamp(name).toInstant.atZone(timeZone).toOffsetDateTime
  }

  implicit def offsetTimeBinder(implicit timeZone: ZoneId) = new ColumnBinder[OffsetTime]{
    override val jdbcType: Int = Types.TIME
    override def set(value: OffsetTime, stmt: PreparedStatement, i: Int): Unit = stmt.setTime(i + 1, Time.valueOf(value.toLocalTime))
    override def get(name: String, rs: ResultSet): OffsetTime = rs.getTime(name).toInstant.atZone(timeZone).toOffsetDateTime.toOffsetTime
  }

  implicit def instantBinder(implicit timeZone: ZoneId) = new ColumnBinder[Instant]{
    override val jdbcType: Int = Types.TIMESTAMP
    override def set(value: Instant, stmt: PreparedStatement, i: Int): Unit = stmt.setTimestamp(i + 1, new Timestamp(value.toEpochMilli))
    override def get(name: String, rs: ResultSet): Instant = rs.getTimestamp(name).toInstant
  }

  trait ColumnBinder[T] {
    val jdbcType: Int
    def set(value: T, stmt: PreparedStatement, i: Int): Unit
    def get(name: String, rs: ResultSet): T
  }

  implicit def toSelectColumns[T, S](column: ColumnBase[T, S]): SelectColumns[ColumnBase[T, S], S] = SelectColumns(column, Seq(column), column.get)
  implicit def toSelectColumns[T <: TableDef[_], R](table: T): SelectColumns[T, R] = SelectColumns(table, table.columns, (table.toModel _).asInstanceOf[ResultSet => R])
  implicit def toGroupingColumns[T, S](column: ColumnBase[T, S]): GroupingColumns[ColumnBase[T, S], S] = GroupingColumns(column, Seq(GroupingColumn(column)), column.get)

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

