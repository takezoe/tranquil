package models

import com.github.takezoe.tranquil._

case class CompanyInfo(companyId: Int, companyName: String)

class CompanyInfoTableDef extends TableDef[CompanyInfo]("COMPANY_INFO"){
  val companyId = new Column[Int](this, "COMPANY_ID")
  val companyName = new Column[String](this, "COMPANY_NAME")
  override def toModel(rs: java.sql.ResultSet): CompanyInfo = {
    CompanyInfo(companyId.get(rs), companyName.get(rs))
  }
}

case class UserInfo(userId: Int, userName: String, companyId: Option[Int])

class UserInfoTableDef extends TableDef[UserInfo]("USER_INFO"){
  val userId = new Column[Int](this, "USER_ID")
  val userName = new Column[String](this, "USER_NAME")
  val companyId = new OptionalColumn[Int](this, "COMPANY_ID")
  override def toModel(rs: java.sql.ResultSet): UserInfo = {
    UserInfo(userId.get(rs), userName.get(rs), companyId.get(rs))
  }
}

object Tables {
  val CompanyInfo = new SingleTableAction[CompanyInfoTableDef, CompanyInfo](new CompanyInfoTableDef())
  val UserInfo = new SingleTableAction[UserInfoTableDef, UserInfo](new UserInfoTableDef())
}
