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

//  Class.forName("org.h2.Driver")
//  val conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/data", "sa", "sa")

  val query1 = Users()
    .insert { u =>
      (u.userId    -> "takezoe") ~
      (u.userName  -> "Naoki Takezoe") ~
      (u.companyId -> 1)
    }

  println(query1.insertStatement())

  val query2 = Users()
    .update { u =>
      (u.userName  -> "N. Takezoe") ~
      (u.companyId -> 1)
    }
    .filter(_.userId eq "takezoe")

  println(query2.updateStatement())

  val query3 = Users().delete()
    .filter(_.userId eq "takezoe")

  println(query3.deleteStatement())

//  query.list(conn).foreach { case u ~ c =>
//      println(u.userId + " " + u.userName + " " + c.map(_.companyName).getOrElse(""))
//  }
//
//  conn.close()
}

case class User(userId: String, userName: String, companyId: Option[Int])

class Users(val alias: Option[String]) extends TableDef[User] {
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
  val companyId   = new Column[Int](alias, "COMPANY_ID")
  val companyName = new Column[String](alias, "COMPANY_NAME")
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

