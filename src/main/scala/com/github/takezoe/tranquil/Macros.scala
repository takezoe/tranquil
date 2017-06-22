package com.github.takezoe.tranquil

import scala.meta._

class table(tableName: String) extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    val tableName = this match {
      case q"new table(${Lit.String(s)})" => s
    }

    defn match {
      case t @ q"case class $name(..$props)" =>
        TableMacroImpl.expand(t, tableName, name, props)
      case _ =>
        abort("@table must annotate an case class.")
    }
  }
}

object TableMacroImpl {
  def expand[T](t: Stat, tableName: String, name: Type.Name, props: scala.collection.immutable.Seq[Term.Param]): Term = {
    val fields = props.map { p =>
      param"${Term.Name(p.name.value)}: Column[${p.decltpe.get.asInstanceOf[Type.Name]}]"
    }

    val mappers = props.map { p =>
      q"${Term.Name(p.name.value)}.get(rs)"
    }

    val columns = props.map { p =>
      q"new Column[${p.decltpe.get.asInstanceOf[Type.Name]}](alias, ${Lit.String(camelToSnake(p.name.value))})"
    }

    val objectName = snakeToCamel(tableName)

    Term.Block(scala.collection.immutable.Seq(
      t,
      q"""case class ${Type.Name(objectName)}(alias: Option[String], ..$fields) extends TableDef[$name](${Lit.String(tableName)}){
        def toModel(rs: java.sql.ResultSet): $name = { new ${Ctor.Ref.Name(name.value)}(..$mappers) }
      }""",
      q"""object ${Term.Name(objectName)}{
        def apply() = new SingleTableAction[${Type.Name(objectName)}](table(None))
        def apply(alias: String) = new Query[${Type.Name(objectName)}, ${Type.Name(objectName)}, $name](table(Some(alias)))
        private def table(alias: Option[String]) = {
          new ${Ctor.Ref.Name(objectName)}(alias, ..$columns)
        }
      }"""
    ))
  }

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

  private def snakeToCamel(name: String): String = {
    val sb = new StringBuilder()
    var upper = true
    name.foreach { c =>
      if(c == '_'){
        upper = true
      } else if(upper){
        sb.append(Character.toUpperCase(c))
        upper = false
      } else {
        sb.append(Character.toLowerCase(c))
      }
    }
    sb.toString
  }

}