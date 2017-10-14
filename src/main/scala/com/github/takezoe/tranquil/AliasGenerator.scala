package com.github.takezoe.tranquil

class AliasGenerator {
  private var counter = 0

  def generate(): String = {
    counter = counter + 1
    "x%02d".format(counter)
  }
}
