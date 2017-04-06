package com.github.takezoe.sqlbuilder

import scala.collection.mutable.ListBuffer

class BindParams {

  private val list = new ListBuffer[Param[_]]

  def ++=(param: Seq[Param[_]]): BindParams = {
    list ++= param
    this
  }

  def params: Seq[Param[_]] = list.toSeq

}
