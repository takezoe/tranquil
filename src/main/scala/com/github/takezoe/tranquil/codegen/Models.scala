package com.github.takezoe.tranquil.codegen

object Models {

  /**
   * The table model.
   *
   * @param name the table name
   * @param columns the list of column
   */
  case class Table(name: String, columns: List[Column]){
    val className: String = uppercamel(name)
  }

  /**
   * The column model.
   *
   * @param name the column name
   * @param typeName the column type
   * @param dataType the date type of the column
   * @param nullable the nullable flag
   * @param primaryKey true if it is a primary key
   */
  case class Column(name: String, typeName: String, dataType: String, nullable: Boolean, primaryKey: Boolean){
    val propertyName: String = lowercamel(name)
  }

  private def lowercamel(str: String): String = {
    str.toLowerCase().split("_").zipWithIndex.map {
      case (word, i) =>
        i match {
          case 0 => word
          case _ => word.capitalize
        }
    }.mkString("")
  }

  private def uppercamel(str: String): String = {
    str.toLowerCase().split("_").map { _.capitalize }.mkString("")
  }

}
