# Tranquil [![Build Status](https://travis-ci.org/takezoe/tranquil.svg?branch=master)](https://travis-ci.org/takezoe/tranquil)

Tranquil an experiment of type-safe SQL builder for Scala.

```scala
libraryDependencies += "com.github.takezoe" %% "tranquil" % "1.0.0"
```

## Usage

First, ready table definitions like following:

```scala
import com.github.takezoe.tranquil._
import java.sql.ResultSet

case class User(userId: String, userName: String, companyId: Option[Int])

class Users extends TableDef[User]("USERS") {
  val userId    = new Column[String](this, "USER_ID")
  val userName  = new Column[String](this, "USER_NAME")
  val companyId = new OptionalColumn[Int](this, "COMPANY_ID")

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

case class Company(companyId: Int, companyName: String)

class Companies extends TableDef[Company]("COMPANIES") {
  val companyId   = new Column[Int](this, "COMPANY_ID")
  val companyName = new Column[String](this, "COMPANY_NAME")

  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Tables {
  val Users = new SingleTableAction[Users, User](new Users())
  val Companies = new SingleTableAction[Companies, Company](new Companies())
}
```

Then you can assemble SQL using type-safe DSL.

```scala
import com.github.takezoe.tranquil._
import com.github.takezoe.tranquil.Dialect.generic
import Tables._

val conn: java.sql.Connection = ...

// SELECT
val users: Seq[(User, Option[Company])] =
  Users
    .leftJoin(Companies){ case u ~ c => u.companyId eq c.companyId }
    .filter { case u ~ c => (u.userId eq "takezoe") || (u.userId eq "takezoen") }
    .sortBy { case u ~ c => u.userId asc }
    .list(conn)
```

Generated SQL is:

```sql
SELECT
  u.USER_ID      AS u_USER_ID,
  u.USER_NAME    AS u_USER_NAME,
  u.COMPANY_ID   AS u_COMPANY_ID,
  c.COMPANY_ID   AS c_COMPANY_ID,
  c.COMPANY_NAME AS c_COMPANY_NAME
FROM USERS u
LEFT JOIN COMPANIES c ON u.COMPANY_ID = c.COMPANY_ID
WHERE (u.USER_ID = ? OR u.USER_ID = ?)
ORDER BY u.USER_ID ASC
```

Grouping and aggregation are possible as follows:

```scala
val counts: Seq[(Option[Int], Long)] = 
  Users
    .groupBy { t => t.companyId ~ t.userId.count }
    .filter  { case companyId ~ count => count ge 10 }
    .list(conn)
```

Also you can assemble insert, update and delete SQL in the same way.

```scala
// INSERT
Users
  .insert { u => 
    (u.userId -> "takezoe") ~ 
    (u.userName -> "Naoki Takezoe")
  }
  .execute(conn)

// UPDATE
Users
  .update(_.userName -> "N. Takezoe")
  .filter(_.userId eq "takezoe")
  .execute(conn)

// DELETE
Users
  .delete()
  .filter(_.userId eq "takezoe")
  .execute(conn)
```

## Code generator

Tranquil has a code generator to generate case classes and table definitions like explained above from database schema. You can run the code generator as follows:

```scala
import com.github.takezoe.tranquil.codegen._

CodeGenerator.generate(Settings(
  url       = "jdbc:h2:mem:test;TRACE_LEVEL_FILE=4",
  driver    = "org.h2.Driver",
  username  = "sa",
  password  = "sa"
))
```

Source files would be generated into `/src/main/scala/models/Tables.scala` in default.

In addition, you can configure the code generator via `Settings` which is passed to `CodeGenerator`. `Settings` has following properties:

property           | type            | description
-------------------|-----------------|------------------------------------------------
driver             | String          | JDBC driver classname (required)
url                | String          | JDBC connection url (required)
username           | String          | JDBC connection username (required)
password           | String          | JDBC connection password (required)
catalog            | String          | catalog (default is "%")
schemaPattern      | String          | schema pattern (default is "%")
tablePattern       | String          | table pattern (default is "%")
includeTablePattern| String          | regular expression which matches included tables (default is "")
excludeTablePattern| String          | regular expression which matches excluded tables (default is "")
packageName        | String          | package name of generated source (default is "models")
targetDir          | File            | output directory of generated source (default is new File("src/main/scala"))
charset            | String          | chaarset of generated source (default is "UTF-8")
typeMappings       | Map[Int, String]| mappings of SQL type to Scala type (default is DataTypes.defaultMappings)
