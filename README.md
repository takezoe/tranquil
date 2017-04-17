# Tranquil

Tranquil is a experiment of type-safe SQL builder for Scala.

```scala
libraryDependencies += "com.github.takezoe" %% "tranquil" % "0.0.2"
```

At first, ready table definitions like following:

```scala
import com.github.takezoe.sqlbuilder._

case class User(userId: String, userName: String, companyId: Option[Int])

class Users(val alias: Option[String]) extends TableDef[User] {
  val tableName = "USERS"
  val userId    = Column[String](alias, "USER_ID")
  val userName  = Column[String](alias, "USER_NAME")
  val companyId = Column[Int](alias, "COMPANY_ID", nullable = true)
  val columns = Seq(userId, userName, companyId)

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.getOpt(rs))
  }
}

object Users {
  def apply() = {
    val users = new Users(None)
    new SingleTableAction[Users](users)
  }
  def apply(alias: String) = {
    val users = new Users(Some(alias))
    new Query[Users, Users, User](users, users, users.toModel _)
  }
}

case class Company(companyId: Int, companyName: String)

class Companies(val alias: Option[String]) extends TableDef[Company] {
  val tableName = "COMPANIES"
  val companyId   = Column[Int](alias, "COMPANY_ID")
  val companyName = Column[String](alias, "COMPANY_NAME")
  val columns = Seq(companyId, companyName)

  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Companies {
  def apply() = {
    val companies = new Companies(None)
    new SingleTableAction[Companies](companies)
  }
  def apply(alias: String) = {
    val companies = new Companies(Some(alias))
    new Query[Companies, Companies, Company](companies, companies, companies.toModel _)
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

Also you can assemble insert, update and delete SQl in the same way.

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
