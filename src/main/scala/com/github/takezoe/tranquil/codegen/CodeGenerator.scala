package com.github.takezoe.tranquil.codegen

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * Generate case classes and table definitions from JDBC metadata.
 */
object CodeGenerator {

  def generate(settings: Settings): Unit = {
    generateSources(settings, new SchemaLoader(settings).loadSchema())
  }

  private def generateSources(settings: Settings, tables: List[Models.Table]): Unit = {
    import settings._
    val sb = new StringBuilder()

    sb.append(s"package ${packageName}\n")
    sb.append("\n")
    sb.append("import com.github.takezoe.tranquil._\n")
    sb.append("\n")

    val outputDir = packageName match {
      case "" => targetDir
      case _  => new File(targetDir, packageName.replace(".", "/"))
    }
    outputDir.mkdirs()

    tables.foreach { table =>
      val source = generateSource(settings, table)
      sb.append(source)
    }

    sb.append("object Tables {\n")
    tables.foreach { table =>
      sb.append(s"  val ${table.className} = new SingleTableAction[${table.className}TableDef, ${table.className}](new ${table.className}TableDef())\n")
    }
    sb.append("}\n")

    val file = new File(outputDir, "Tables.scala")
    Files.write(file.toPath, sb.toString.getBytes(StandardCharsets.UTF_8))
  }

  private def generateSource(settings: Settings, table: Models.Table): String = {
    val sb = new StringBuilder()
    sb.append(s"case class ${table.className}(")
    sb.append(table.columns.map { column =>
      if(column.nullable){
        s"${column.propertyName}: Option[${column.dataType}]"
      } else {
        s"${column.propertyName}: ${column.dataType}"
      }
    }.mkString(", "))
    sb.append(")\n")

    sb.append("\n")

    sb.append(s"""class ${table.className}TableDef extends TableDef[${table.className}]("${table.name}"){""")
    sb.append("\n")
    sb.append(table.columns.map { column =>
      if(column.nullable){
        s"""  val ${column.propertyName} = new OptionalColumn[${column.dataType}](this, "${column.name}")"""
      } else {
        s"""  val ${column.propertyName} = new Column[${column.dataType}](this, "${column.name}")"""
      }
    }.mkString("\n"))
    sb.append("\n")
    sb.append(s"  override def toModel(rs: java.sql.ResultSet): ${table.className} = {\n")
    sb.append(s"    ${table.className}(")
    sb.append(table.columns.map { columns => s"${columns.propertyName}.get(rs)" }.mkString(", "))
    sb.append(")\n")
    sb.append("  }\n")
    sb.append("}\n")

    sb.append("\n")
    sb.toString
  }

}
