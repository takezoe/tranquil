package com.github.takezoe.tranquil

import java.sql.ResultSet

/**
 * A base class for table definitions which are used for DSL.
 */
abstract class TableDef[R <: Product](val tableName: String) extends Product {

  lazy val columns: Seq[ColumnBase[_, _]] = {
    productIterator.collect { case c: ColumnBase[_, _] => c }.toSeq
  }

  val alias: Option[String]

  def wrap(alias: String): this.type = {
    val cols = columns
    val constructor = getClass.getDeclaredConstructor(
      classOf[Option[String]] +: cols.map(_.getClass): _*
    )
    constructor.newInstance(Option(alias) +: cols.map(_.wrap(alias)): _*).asInstanceOf[this.type]
  }

  def toModel(rs: ResultSet): R

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
