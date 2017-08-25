package com.github.takezoe.tranquil

import java.util.concurrent.atomic.AtomicInteger

object AliasGenerator {
  private val MaxValue = 99
  private val counter = new AtomicInteger(0)

  def generate(): String = {
    "x%02d".format(counter.accumulateAndGet(1, (index, _) => {
      if(index + 1 >= MaxValue) 0 else index + 1
    }))
  }
}
