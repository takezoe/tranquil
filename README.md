# Tranquil [![Build Status](https://travis-ci.org/takezoe/tranquil.svg?branch=master)](https://travis-ci.org/takezoe/tranquil)

Tranquil is a experiment of type-safe SQL builder for Scala.

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.github.takezoe" %% "tranquil" % "0.0.3-SNAPSHOT"
```

At first, ready table definitions like following:

```scala
import com.github.takezoe.tranquil._
import java.sql.ResultSet

case class User(userId: String, userName: String, companyId: Option[Int])

class Users extends TableDef[User]("USERS") {
  val userId    = new Column[String](this, "USER_ID"),
  val userName  = new Column[String](this, "USER_NAME"),
  val companyId = new OptionalColumn[Int](this, "COMPANY_ID")

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

object Users {
  def apply() = new SingleTableAction[Users](new Users())
}

case class Company(companyId: Int, companyName: String)

class Companies extends TableDef[Company]("COMPANIES") {
  val companyId   = new Column[Int](this, "COMPANY_ID"),
  val companyName = new Column[String](this, "COMPANY_NAME")

  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Companies {
  def apply() = new SingleTableAction[Companies](new Companies())
}
```

Then you can assemble SQL using type-safe DSL.

```scala
import com.github.takezoe.tranquil._
import com.github.takezoe.tranquil.Dialect.generic

val conn: java.sql.Connection = ...

// SELECT
val users: Seq[(User, Option[Company])] =
  Users()
    .leftJoin(Companies()){ case u ~ c => u.companyId eq c.companyId }
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
  Users()
    .groupBy { t => t.companyId ~ t.userId.count }
    .filter  { case companyId ~ count => count ge 10 }
    .list(conn)
```

Also you can assemble insert, update and delete SQL in the same way.

```scala
// INSERT
Users()
  .insert { u => 
    (u.userId -> "takezoe") ~ 
    (u.userName -> "Naoki Takezoe")
  }
  .execute(conn)

// UPDATE
Users()
  .update(_.userName -> "N. Takezoe")
  .filter(_.userId eq "takezoe")
  .execute(conn)

// DELETE
Users()
  .delete()
  .filter(_.userId eq "takezoe")
  .execute(conn)
```
