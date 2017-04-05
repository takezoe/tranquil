# scala-sqlbuilder

This is a experiment of type-safe SQL builder for Scala.

At first, ready table definitions like this:

```scala
class Users(val alias: String) extends TableDefinition {
  val tableName = "USERS"
  val userId    = new Column[String](alias, "USER_ID")
  val userName  = new Column[String](alias, "USER_NAME")
  val companyId = new Column[Int](alias, "COMPANY_ID")
  val columns = Seq(userId, userName, companyId)
}

object Users {
  def apply(alias: String) = new Query[Users, Users](new Users(alias), new Users(alias))
}

class Companies(val alias: String) extends TableDefinition {
  val tableName = "COMPANIES"
  val companyId   = new Column[Int](alias, "COMPANY_ID")
  val companyName = new Column[String](alias, "COMPANY_NAME")
  val columns = Seq(companyId, companyName)
}

object Companies {
  def apply(alias: String) = new Query[Companies, Companies](new Companies(alias), new Companies(alias))
}
```

Then you can assemble SQL using type-safe DSL.

```scala
val query = Users("u")
  .innerJoin(Companies("c")){ case u ~ c => u.companyId == c.companyId }
  .filter { case u ~ c => (u.userId == "takezoe") || (u.userId == "takezoen") }
  .sortBy { case u ~ c => u.userId asc }
  .map    { case u ~ c => u.userName ~ c.companyName}

println(query.sql)
```
