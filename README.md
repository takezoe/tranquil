# scala-sqlbuilder

This is a experiment of type-safe SQL builder for Scala.

At first, ready table definitions like this:

```scala
case class User(userId: String, userName: String, companyId: Option[Int])

class Users(val alias: String) extends TableDef[User] {
  val tableName = "USERS"
  val userId    = new Column[String](alias, "USER_ID")
  val userName  = new Column[String](alias, "USER_NAME")
  val companyId = new Column[Int](alias, "COMPANY_ID")
  val columns = Seq(userId, userName, companyId)

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.getOpt(rs))
  }
}

object Users {
  def apply(alias: String) = {
    val users = new Users(alias)
    new Query[Users, Users, User](users, users, users.toModel _)
  }
}

case class Company(companyId: Int, companyName: String)

class Companies(val alias: String) extends TableDef[Company] {
  val tableName = "COMPANIES"
  val companyId   = new Column[Int](alias, "COMPANY_ID")
  val companyName = new Column[String](alias, "COMPANY_NAME")
  val columns = Seq(companyId, companyName)

  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Companies {
  def apply(alias: String) = {
    val companies = new Companies(alias)
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

// INSERT
Users()
  .insert { u => (u.userId -> "takezoe") ~ (u.userName -> "Naoki Takezoe") }
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
