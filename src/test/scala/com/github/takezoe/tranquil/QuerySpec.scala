package com.github.takezoe.tranquil

import java.sql._
import org.scalatest.FunSuite

class QuerySpec extends FunSuite {

  test("leftJoin"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies().insert(_.companyName -> "BizReach").execute(conn)
      Users().insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)
      Users().insert(u => (u.userName -> "employee2") ~ (u.companyId -> 1)).execute(conn)
      Users().insert(u => (u.userName -> "employee3")).execute(conn)

      val results = Users("u")
        .leftJoin(Companies("c")){ case u ~ c => u.companyId eq c.companyId }
        .filter { case u ~ c => c.companyName eq "BizReach" }
        .sortBy { case u ~ c => u.userId.asc }
        .list(conn)

      assert(results.size == 2)

      results.zipWithIndex.foreach { case user ~ company ~ i =>
        assert(user.userName == "employee" + (i + 1))
        assert(company.get.companyName == "BizReach")
      }
    } finally {
      conn.close()
    }
  }

  test("count"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Users().insert(_.userName -> "takezoe").execute(conn)
      Users().insert(_.userName -> "n.takezoe").execute(conn)

      val query = Users("u")
        .filter(_.userName eq "takezoe")
        .map { t => t ~ t.companyId }

      val result = query.list(conn)

      assert(result.size == 1)
      assert(result == Seq((User("1", "takezoe", None), None)))
    } finally {
      conn.close()
    }
  }

  private def createTables(conn: Connection) = {
    executeSql(conn,
      """CREATE TABLE COMPANIES (
        |COMPANY_ID   SERIAL       PRIMARY KEY,
        |COMPANY_NAME VARCHAR(200) NOT NULL
        |)""".stripMargin)

    executeSql(conn,
      """CREATE TABLE USERS (
        |USER_ID    SERIAL       PRIMARY KEY,
        |USER_NAME  VARCHAR(200) NOT NULL,
        |COMPANY_ID INT,
        |FOREIGN KEY(COMPANY_ID) REFERENCES COMPANIES(COMPANY_ID)
        |)""".stripMargin)
  }

  private def executeSql(conn: Connection, sql: String) = {
    val stmt = conn.createStatement()
    try {
      stmt.executeUpdate(sql)
    } finally {
      stmt.close()
    }
  }

}

case class User(userId: String, userName: String, companyId: Option[Int])

class Users(val alias: Option[String]) extends TableDef[User] {
  val tableName = "USERS"
  val userId    = new Column[String](alias, "USER_ID")
  val userName  = new Column[String](alias, "USER_NAME")
  val companyId = new OptionalColumn[Int](alias, "COMPANY_ID")
  val columns = Seq(userId, userName, companyId)

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

object Users {
  def apply() = {
    val users = new Users(None)
    new SingleTableAction[Users](users)
  }
  def apply(alias: String) = {
    val users = new Users(Some(alias))
    new Query[Users, Users, User](users)
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
    new Query[Companies, Companies, Company](companies)
  }
}
