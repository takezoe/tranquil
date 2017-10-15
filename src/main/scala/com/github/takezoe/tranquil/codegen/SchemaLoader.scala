package com.github.takezoe.tranquil.codegen

import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet}

import com.github.takezoe.tranquil.using

class SchemaLoader(settings: Settings) {
    import settings._

  implicit class IterableResultSet(rs: ResultSet){
    def map[T](handler: (ResultSet) => T): Seq[T] = rs.next match {
      case false => Stream.empty
      case true  => handler(rs) +: map(handler)
    }
  }

  def loadSchema(): List[Models.Table] = {
    Class.forName(settings.driver)

    using(DriverManager.getConnection(url, username, password)){ conn =>
      using(conn.getMetaData().getTables(catalog, schemaPattern, tablePattern, null)){ rs =>
        rs.map { rs =>
          rs.getString("TABLE_TYPE") match {
            case "TABLE" => Some(rs.getString("TABLE_NAME"))
            case _ => None
          }
        }.flatten.filterNot { tableName =>
          (includeTablePattern.nonEmpty && !tableName.matches(includeTablePattern)) ||
            (excludeTablePattern.nonEmpty && tableName.matches(excludeTablePattern))
        }.map { name => loadTable(conn, name) }.toList
      }
    }
  }

  protected def loadTable(conn: Connection, name: String): Models.Table = {
    val primaryKeys = using(conn.getMetaData().getPrimaryKeys(catalog, schemaPattern, name)){ rs =>
      rs.map { _.getString("COLUMN_NAME") }
    }

    val columns = using(conn.getMetaData().getColumns(catalog, schemaPattern, name, "%")){ rs =>
      rs.map { rs =>
        try {
          val columnName = rs.getString("COLUMN_NAME")
          val dataType = rs.getInt("DATA_TYPE")

          Some(Models.Column(
            columnName,
            rs.getString("TYPE_NAME"),
            typeMappings.getOrElse(dataType,{
              DataTypes.toSqlType(dataType) match {
                case "UNKNOWN" => throw new IllegalArgumentException("%i is unknown type.".format(dataType))
                case sqlType   => throw new IllegalArgumentException("%s is not supported.".format(sqlType))
              }
            }),
            rs.getInt("NULLABLE") match {
              case DatabaseMetaData.columnNullable => true
              case _ => false
            },
            primaryKeys.contains(columnName)
          ))

        } catch {
          case ex: IllegalArgumentException => None
        }
      }.flatten.toList
    }

    Models.Table(name, columns)
  }

}
