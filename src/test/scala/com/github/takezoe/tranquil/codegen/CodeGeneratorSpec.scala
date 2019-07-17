package com.github.takezoe.tranquil.codegen

import java.sql.{Connection, DriverManager}
import java.io.{File, FileInputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.FunSuite
import scala.util.Try

class CodeGeneratorSpec extends FunSuite {

  // TODO workaround for test failure in cross build
  Try {
    DriverManager.getConnection("jdbc:h2:mem:test-codegen;TRACE_LEVEL_FILE=4")
  }
  
  test("CodeGenerator"){
    val targetDir = new File("test")
    try {
      val conn = DriverManager.getConnection("jdbc:h2:mem:test-codegen;TRACE_LEVEL_FILE=4")
      try {
        createTables(conn)
        CodeGenerator.generate(Settings(
          url       = "jdbc:h2:mem:test-codegen;TRACE_LEVEL_FILE=4",
          driver    = "org.h2.Driver",
          username  = "",
          password  = "",
          targetDir = targetDir
        ))
      } finally {
        conn.close()
      }

      val result = IOUtils.toString(new FileInputStream(new File("test/models/Tables.scala")), "UTF-8")
      val expect = IOUtils.toString(getClass.getClassLoader.getResourceAsStream("com/github/takezoe/tranquil/codegen/Tables.scala"), "UTF-8")

      assert(result == expect)
    } finally {
      if(targetDir.exists){
        FileUtils.deleteQuietly(targetDir)
      }
    }
  }

  private def createTables(conn: Connection) = {
    executeSql(conn,
      """CREATE TABLE COMPANY_INFO (
        |COMPANY_ID   SERIAL       PRIMARY KEY,
        |COMPANY_NAME VARCHAR(200) NOT NULL
        |)""".stripMargin)

    executeSql(conn,
      """CREATE TABLE USER_INFO (
        |USER_ID    SERIAL       PRIMARY KEY,
        |USER_NAME  VARCHAR(200) NOT NULL,
        |COMPANY_ID INT,
        |FOREIGN KEY(COMPANY_ID) REFERENCES COMPANY_INFO(COMPANY_ID)
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
