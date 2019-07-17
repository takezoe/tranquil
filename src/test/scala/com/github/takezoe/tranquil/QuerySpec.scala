package com.github.takezoe.tranquil

import java.sql._
import org.scalatest.FunSuite
import com.github.takezoe.tranquil.Dialect.generic
import scala.util.Try

import Tables._

class QuerySpec extends FunSuite {

  // TODO workaround for test failure in cross build
  Try {
    DriverManager.getConnection("jdbc:h2:mem:test-codegen;TRACE_LEVEL_FILE=4")
  }

  test("leftJoin"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies.insert(_.companyName -> "BizReach").execute(conn)
      Users.insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee2") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee3")).execute(conn)

      val results = Users
        .leftJoin(Companies){ case u ~ c => u.companyId eq c.companyId }
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
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Users.insert(User(Default[Long], "takezoe", None)).execute(conn)
      Users.insert(User(Default[Long], "n.takezoe", None)).execute(conn)

      val query = Users
        .filter(_.userName eq "takezoe")
        .map { t => t ~ t.companyId }

      val result = query.list(conn)

      assert(result.size == 1)
      assert(result == Seq((User(1, "takezoe", None), None)))
    } finally {
      conn.close()
    }
  }

  test("in"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Users.insert(_.userName -> "user1").execute(conn)
      Users.insert(_.userName -> "user2").execute(conn)
      Users.insert(_.userName -> "user3").execute(conn)

      val query = Users
        .filter(_.userName in Seq("user1", "user2"))

      val result = query.list(conn)

      assert(result.size == 2)
    } finally {
      conn.close()
    }
  }

  test("in with subquery"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies.insert(_.companyName -> "BizReach").execute(conn)
      Users.insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee2") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee3")).execute(conn)

      val query = Users
        .filter(_.companyId in (Companies.filter(_.companyId eq 1).map(_.companyId)))

      val result = query.list(conn)

      assert(result.size == 2)
    } finally {
      conn.close()
    }
  }

  test("groupBy"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies.insert(_.companyName -> "BizReach").execute(conn)
      Users.insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee2") ~ (u.companyId -> 1)).execute(conn)
      Users.insert(u => (u.userName -> "employee3")).execute(conn)

      val results = Users
        .groupBy { t => t.companyId ~ t.userId.count }
        .filter { case _ ~ count => count ge 1 }
        .list(conn)

      assert(results == Seq((None, 1), (Some(1), 2)))
    } finally {
      conn.close()
    }
  }

  test("subquery in condition"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies.insert(_.companyName -> "BizReach").execute(conn)
      Users.insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)

      val results = Users
        .filter(_.companyId eq (
          Companies.filter(_.companyName eq "BizReach").map(_.companyId)
        ))
        .list(conn)

      assert(results == Seq(User(1, "employee1", Some(1))))
    } finally {
      conn.close()
    }
  }

  test("subquery in join"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies.insert(_.companyName -> "BizReach").execute(conn)
      Users.insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)

      val subquery1 = Companies
        .filter(_.companyName eq "BizReach")

      val subquery2 = Companies
        .filter(_.companyName eq "BizReach")
        .map { t => t.companyId ~ t.companyName }

      val results = Users
        .innerJoin(subquery1){ case u ~ c => u.companyId eq c.companyId }
        .leftJoin(subquery2){ case _ ~ c ~ (companyId ~ _) => c.companyId eq companyId }
        .list(conn)

      assert(results == Seq(((User(1, "employee1", Some(1)), Company(1, "BizReach")), Some((1, "BizReach")))))
    } finally {
      conn.close()
    }
  }

  test("dialect function"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Users.insert(u => (u.userName -> "takezoe")).execute(conn)

      val query = Users
        .filter(_.userName.toUpperCase eq "TAKEZOE")
        .map(_.userName.toUpperCase)

      val result = query.list(conn)

      assert(result == Seq("TAKEZOE"))
    } finally {
      conn.close()
    }
  }

  test("returning"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      val id1 = Users.insert(_.userName -> "user1").executeAndReturnGeneratedId(conn)
      val id2 = Users.insert(_.userName -> "user2").executeAndReturnGeneratedId(conn)

      assert(id1 == 1)
      assert(id2 == 2)
    } finally {
      conn.close()
    }
  }

  test("like operator"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test-query;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Users.insert(_.userName -> "takezoe").execute(conn)
      Users.insert(_.userName -> "tak_zoe").execute(conn)

      val results1 = Users.filter(_.userName.startsWith("tak")).list(conn)
      assert(results1 == Seq(User(1, "takezoe", None), User(2, "tak_zoe", None)))

      val results2 = Users.filter(_.userName.endsWith("zoe")).list(conn)
      assert(results2 == Seq(User(1, "takezoe", None), User(2, "tak_zoe", None)))

      val results3 = Users.filter(_.userName.contains("kez")).list(conn)
      assert(results3 == Seq(User(1, "takezoe", None)))

      val results4 = Users.filter(_.userName.contains("k_z")).list(conn)
      assert(results4 == Seq(User(2, "tak_zoe", None)))

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

case class User(userId: Long, userName: String, companyId: Option[Long])

class Users extends TableDef[User]("USERS") {
  val userId = new Column[Long](this, "USER_ID", true)
  val userName = new Column[String](this, "USER_NAME")
  val companyId = new OptionalColumn[Long](this, "COMPANY_ID")

  override def toModel(rs: ResultSet): User = {
    User(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

case class Company(companyId: Long, companyName: String)

class Companies extends TableDef[Company]("COMPANIES") {

  val companyId = new Column[Long](this, "COMPANY_ID", true)
  val companyName = new Column[String](this, "COMPANY_NAME")

  override def toModel(rs: ResultSet): Company = {
    Company(companyId.get(rs), companyName.get(rs))
  }
}

object Tables {
  val Users = new SingleTableAction[Users, User](new Users())
  val Companies = new SingleTableAction[Companies, Company](new Companies())
}
