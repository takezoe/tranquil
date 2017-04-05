package com.github.takezoe.sqlbuilder

object SqlBuilderTest extends App {

  val query = Users("u")
    .innerJoin(Companies("c1")){ case u ~ c => u.companyId == c.companyId }
    .innerJoin(Companies("c2")){ case u ~ c1 ~ c2 => u.companyId == c2.companyId }
    .filter { case u ~ c1 ~ c2 => (u.userId == "123") || (u.userId == "456") }
    .sortBy { case u ~ c1 ~ c2 => u.userId asc }
    .map    { case u ~ c1 ~ c2 => u.userName ~ c1.companyName}

  query.toSql() match {
    case (sql, params) => {
      println(sql)
      println(params.map(_.value))
    }
  }

}

class Users(val alias: String) extends TableDef {
  val tableName = "USERS"
  val userId    = new Column[String](alias, "USER_ID")
  val userName  = new Column[String](alias, "USER_NAME")
  val companyId = new Column[Int](alias, "COMPANY_ID")
  val columns = Seq(userId, userName, companyId)
}

object Users {
  def apply(alias: String) = new Query[Users, Users](new Users(alias), new Users(alias))
}

class Companies(val alias: String) extends TableDef {
  val tableName = "COMPANIES"
  val companyId   = new Column[Int](alias, "COMPANY_ID")
  val companyName = new Column[String](alias, "COMPANY_NAME")
  val columns = Seq(companyId, companyName)
}

object Companies {
  def apply(alias: String) = new Query[Companies, Companies](new Companies(alias), new Companies(alias))
}

