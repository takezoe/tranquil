package com.github.takezoe.sqlbuilder

import java.sql.{DriverManager, ResultSet}

object SqlBuilderTest extends App {

  val query = Users("u")
    .leftJoin(Companies("c")){ case u ~ c => u.companyId == c.companyId }
    //.filter { case u ~ c1 => (u.userId == "123") || (u.userId == "456") }
    .sortBy { case u ~ c1 => u.userId asc }

  query.toSql() match {
    case (sql, bindParams) => {
      println(sql)
      println(bindParams.params.map(_.value))
    }
  }


  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/data", "sa", "sa")

  query.list(conn).foreach { case u ~ c =>
      println(u.userId + " " + u.userName + " " + c.map(_.companyName).getOrElse(""))
  }

  conn.close()
}

case class User(userId: String, userName: String, companyId: Option[Int])

class Users(val alias: String) extends TableDef[User] {
  val tableName = "USERS"
  val userId    = new Column[String](alias, "ID")
  val userName  = new Column[String](alias, "NAME")
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
  val companyId   = new Column[Int](alias, "ID")
  val companyName = new Column[String](alias, "NAME")
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

