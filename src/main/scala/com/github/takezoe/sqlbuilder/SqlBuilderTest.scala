package com.github.takezoe.sqlbuilder

import java.sql.{DriverManager, ResultSet}

object SqlBuilderTest extends App {

  val query = Users("u")
    .leftJoin(Companies("c")){ case u ~ c => u.companyId eq c.companyId }
    .filter { case u ~ c1 => (u.userId eq "takezoe") || (u.userId eq "takezoen") }
    .sortBy { case u ~ c1 => u.userId asc }

  query.selectStatement() match {
    case (sql, bindParams) => {
      println(sql)
      println(bindParams.params.map(_.value))
    }
  }

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/data", "sa", "sa")

  Users("u")
    .insert { u =>
      (u.userId    -> "takezoe") ~
      (u.userName  -> "Naoki Takezoe") ~
      (u.companyId -> 1)
    }
    .execute(conn)

  Users("u")
    .update
      { u =>
        (u.userName  -> "N. Takezoe") ~
        (u.companyId -> 1)
      }
      { u => u.userId eq "takezoe" }
    .execute(conn)

  query.list(conn).foreach { case u ~ c =>
      println(u.userId + " " + u.userName + " " + c.map(_.companyName).getOrElse(""))
  }

  conn.close()
}

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
    new SingleTableQuery[Users, User](users, users.toModel _)
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
    new SingleTableQuery[Companies, Company](new Companies(alias), companies.toModel _)
  }
}

