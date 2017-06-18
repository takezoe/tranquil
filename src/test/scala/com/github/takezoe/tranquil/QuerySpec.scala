package com.github.takezoe.tranquil

import java.sql._

import org.scalatest.FunSuite
import Tables._

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

  test("groupBy"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies().insert(_.companyName -> "BizReach").execute(conn)
      Users().insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)
      Users().insert(u => (u.userName -> "employee2") ~ (u.companyId -> 1)).execute(conn)
      Users().insert(u => (u.userName -> "employee3")).execute(conn)

      val results = Users("u")
        .groupBy { t => t.companyId ~ t.userId.count }
        .filter { case _ ~ count => count ge 1 }
        .list(conn)

      assert(results == Seq((None, 1), (Some(1), 2)))
    } finally {
      conn.close()
    }
  }

  test("subquery in condition"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies().insert(_.companyName -> "BizReach").execute(conn)
      Users().insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)

      val results = Users("u")
        .filter(_.companyId eq (
          Companies("c").filter(_.companyName eq "BizReach").map(_.companyId)
        ))
        .list(conn)

      assert(results == Seq(User("1", "employee1", Some(1))))
    } finally {
      conn.close()
    }
  }

  test("subquery in join"){
    val conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")
    try {
      createTables(conn)
      Companies().insert(_.companyName -> "BizReach").execute(conn)
      Users().insert(u => (u.userName -> "employee1") ~ (u.companyId -> 1)).execute(conn)

      val subquery1 = Companies("c1")
        .filter(_.companyName eq "BizReach")

      val subquery2 = Companies("c2")
        .filter(_.companyName eq "BizReach")
        .map { t => t.companyId ~ t.companyName }

      val results = Users("u")
        .innerJoin(subquery1, "x1"){ case u ~ c => u.companyId eq c.companyId }
        .leftJoin(subquery2, "x2"){ case _ ~ c ~ (companyId ~ _) => c.companyId eq companyId }
        .list(conn)

      assert(results == Seq(((User("1", "employee1", Some(1)), Company(1, "BizReach")), Some((1, "BizReach")))))
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

