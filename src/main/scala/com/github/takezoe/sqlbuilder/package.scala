package com.github.takezoe

package object sqlbuilder {

  implicit def column2columns(column: Column[_]): Columns = Columns(Seq(column))

  object ~ {
    def unapply[A, B](t: (A, B)): Option[(A, B)] = Some(t)
  }

}
