package com.github.takezoe.tranquil

import java.sql.ResultSet

object Tables {

  case class User(userId: String, userName: String, companyId: Option[Int])

  case class Users(
    alias: Option[String],
    userId: Column[String],
    userName: Column[String],
    companyId: OptionalColumn[Int]
  ) extends TableDef[User]("USERS") {

    override def toModel(rs: ResultSet): User = {
      User(userId.get(rs), userName.get(rs), companyId.get(rs))
    }
  }

  object Users {
    def apply() = new SingleTableAction[Users](table(None))
    def apply(alias: String) = new Query[Users, Users, User](table(Some(alias)))
    private def table(alias: Option[String]) = {
      new Users(
        alias,
        new Column[String](alias, "USER_ID"),
        new Column[String](alias, "USER_NAME"),
        new OptionalColumn[Int](alias, "COMPANY_ID")
      )
    }
  }

  @table("COMPANIES")
  case class Company(companyId: Int, companyName: String)

}
