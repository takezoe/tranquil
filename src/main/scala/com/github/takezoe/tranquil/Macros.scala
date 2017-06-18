package com.github.takezoe.tranquil

import scala.annotation.StaticAnnotation
import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import scala.language.existentials

class table[T](tableName: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro Macros.impl[T]
}

object Macros {

  private def camelToSnake(name: String): String = {
    val sb = new StringBuilder()
    name.foreach { c =>
      if (Character.isUpperCase(c)) {
        if(sb.length != 0){
          sb.append("_")
        }
        sb.append(c)
      } else {
        sb.append(Character.toUpperCase(c))
      }
    }
    sb.toString
  }

  def impl[T](c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val q"""new table[$cls]($tableName).macroTransform($a)""" = c.macroApplication
    val tpe = c.typecheck(q"(??? : $cls)").tpe

    // Extract properties of the case class
    val fields = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramLists.head

    // Generate mapper functions for each properties
    val mappers = fields.map(f => {
      val name = f.name.toTermName
      q"$name.get(rs)"
    })

    // Generate properties of the table definition case class
    val props = fields.map(f => {
      val name = f.name.toTermName
      val returnType = tpe.decl(name).typeSignature
      q"$name: Column[$returnType]" // TODO support option type
    })

    // Generate parameters for creation the table definition instance
    val columns = fields.map(f => {
      val name = f.name.toTermName
      val decoded = camelToSnake(name.decodedName.toString)
      val returnType = tpe.decl(name).typeSignature
      q"new Column[$returnType](alias, $decoded)" // TODO support option type
    })

    // Build the table definition and its companion object
    def modifiedObject(objectDef: ModuleDef): c.Expr[Any] = {
      val ModuleDef(_, objectName, _) = objectDef
      val ret = q"""
case class ${objectName.toTypeName}(
  alias: Option[String],
  ..$props
) extends TableDef[$cls]($tableName){
  override def toModel(rs: ResultSet): $cls = {
    new $cls(..$mappers)
  }
}

object ${objectName.toTermName} {
  def apply() = new SingleTableAction[${objectName.toTypeName}](table(None))
  def apply(alias: String) = new Query[${objectName.toTypeName}, ${objectName.toTypeName}, $cls](table(Some(alias)))
  private def table(alias: Option[String]) = {
    new ${objectName.toTypeName}(
      alias,
      ..$columns
    )
  }
}
      """

      // Debug
      println("==========")
      println(ret)
      println("==========")

      c.Expr[Any](ret)
    }

    annottees.map(_.tree) match {
      case (objectDecl: ModuleDef) :: _ => modifiedObject(objectDecl)
      case x => c.abort(c.enclosingPosition, s"@table can only be applied to an object, not to $x")
    }
  }

}
