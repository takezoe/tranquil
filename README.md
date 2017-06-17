# Tranquil

Tranquil is a experiment of type-safe SQL builder for Scala.

```scala
libraryDependencies += "com.github.takezoe" %% "tranquil" % "0.0.3-SNAPSHOT"
```

At first, ready table definitions like following:

```scala
import com.github.takezoe.tranquil._

case class User(userId: String, userName: String, companyId: Option[Int])

case class Users(
  alias: Option[String],
  userId: Column[String],
  userName: Column[String],
  companyId: OptionalColumn[Int]
) extends TableDef[User]("USERS") {

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

object Users {
  def apply() = new SingleTableAction[Users](table(None))
  def apply(alias: String) = new Query[Users, Users, User](table(Some(alias)))
  private def table(alias: Option[String]) = {
    new Users(
      alias,
      new Column[String](alias, "USER_ID"),
      new Column[String](alias, "USER_NAME"),
      new OptionalColumn[Int](alias, "COMPANY_ID")
    )
  }
}

case class Company(companyId: Int, companyName: String)

case class Companies(
  alias: Option[String],
  companyId: Column[Int],
  companyName: Column[String]
) extends TableDef[Company]("COMPANIES") {
  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Companies {
  def apply() = new SingleTableAction[Companies](table(None))
  def apply(alias: String) = new Query[Companies, Companies, Company](table(Some(alias)))
  private def table(alias: Option[String]) = {
    new Companies(
      alias,
      new Column[Int](alias, "COMPANY_ID"),
      new Column[String](alias, "COMPANY_NAME")
    )
  }
}
```

Then you can assemble SQL using type-safe DSL.

```scala
val conn: java.sql.Connection = ...

// SELECT
val users: Seq[(User, Option[Company])] =
  Users("u")
    .leftJoin(Companies("c")){ case u ~ c => u.companyId eq c.companyId }
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
  Users("u")
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
