package com.github.takezoe.tranquil.codegen

import java.sql.Types._

object DataTypes {

  lazy val defaultMappings = Map[Int, String](
    BIGINT       -> "Int",
    BIT          -> "Int",
    BOOLEAN      -> "Boolean",
    CHAR         -> "String",
    DATE         -> "java.util.Date",
    DECIMAL      -> "Int",
    DOUBLE       -> "Double",
    FLOAT        -> "Float",
    INTEGER      -> "Int",
    LONGNVARCHAR -> "String",
    LONGVARCHAR  -> "String",
    NCHAR        -> "String",
    NUMERIC      -> "Double",
    NVARCHAR     -> "String",
    REAL         -> "Double",
    SMALLINT     -> "Int",
    SQLXML       -> "scala.xml.NodeSeq",
    TIME         -> "java.sql.Time",
    TIMESTAMP    -> "java.sql.Timestamp",
    TINYINT      -> "Int",
    VARCHAR      -> "String"
  )

  def toSqlType(i: Int): String = {
    i match {
      case ARRAY         => "ARRAY"
      case BIGINT        => "BIGINT"
      case BINARY        => "BINARY"
      case BIT           => "BIT"
      case BLOB          => "BLOB"
      case BOOLEAN       => "BOOLEAN"
      case CHAR          => "CHAR"
      case CLOB          => "CLOB"
      case DATALINK      => "DATALINK"
      case DATE          => "DATE"
      case DECIMAL       => "DECIMAL"
      case DISTINCT      => "DISTINCT"
      case DOUBLE        => "DOUBLE"
      case FLOAT         => "FLOAT"
      case INTEGER       => "INTEGER"
      case JAVA_OBJECT   => "JAVAOBJECT"
      case LONGNVARCHAR  => "LONGNVARCHAR"
      case LONGVARBINARY => "LONGVARBINARY"
      case LONGVARCHAR   => "LONGVARCHAR"
      case NCHAR         => "NCHAR"
      case NCLOB         => "NCLOB"
      case NULL          => "NULL"
      case NUMERIC       => "NUMERIC"
      case NVARCHAR      => "NVARCHAR"
      case OTHER         => "OTHER"
      case REAL          => "REAL"
      case REF           => "REF"
      case ROWID         => "ROWID"
      case SMALLINT      => "SMALLINT"
      case SQLXML        => "SQLXML"
      case STRUCT        => "STRUCT"
      case TIME          => "TIME"
      case TIMESTAMP     => "TIMESTAMP"
      case TINYINT       => "TINYINT"
      case VARBINARY     => "VARBINARY"
      case VARCHAR       => "VARCHAR"
      case _             => "UNKNOWN"
    }
  }
}
