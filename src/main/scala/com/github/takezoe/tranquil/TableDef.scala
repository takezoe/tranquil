package com.github.takezoe.tranquil

import java.sql.ResultSet

/**
 * A base class for table definitions which are used for DSL.
 */
abstract class TableDef[R <: Product](val tableName: String){

  lazy val columns: Seq[ColumnBase[_, _]] = getClass.getDeclaredFields.collect { case field
    if classOf[ColumnBase[_, _]].isAssignableFrom(field.getType) =>
      field.setAccessible(true)
      field.get(this).asInstanceOf[ColumnBase[_, _]]
  }.toSeq

  /**
   * An alias of this table.
   */
  private[tranquil] var alias: Option[String] = None

  /**
   * A prefix of columns of this table.
   */
  private[tranquil] var prefix: Option[String] = None

  def wrap(alias: String): this.type = {
    val constructor = getClass.getDeclaredConstructor()
    val tableDef = constructor.newInstance().asInstanceOf[this.type]
    tableDef.alias = Option(alias)
    tableDef.prefix = this.alias.map(_ + "_")
    tableDef
  }

  def renew(alias: String): this.type = {
    val constructor = getClass.getDeclaredConstructor()
    val tableDef = constructor.newInstance().asInstanceOf[this.type]
    tableDef.alias = Option(alias)
    tableDef
  }

  /**
   * Convert a record to an entity.
   */
  def toModel(rs: ResultSet): R

  /**
   * Convert an entity to a sequence of values.
   */
  def fromModel(model: R): Seq[Any] = model.productIterator.toSeq

}

/**
 * Represent a shape of a table. It's used for assemble SQL.
 */
abstract class TableShape[T](table: T){
  val definitions: T = table
  val columns: Seq[ColumnBase[_, _]]
  def wrap(alias: String): TableShape[T]
}

/**
 * Get a TableShape from a table object.
 */
trait TableShapeOf[T] {
  def apply(table: T): TableShape[T]
}
