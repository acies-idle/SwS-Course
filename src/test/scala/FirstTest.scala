package com.aciesidle.training

import org.scalatest.funsuite.AnyFunSuite

/** add vm option to run test
  *   - java.base/sun.util.calendar-ALL-UNNAMED
  */

class FirstTest extends AnyFunSuite {

  test("add (2,3) return 5 ") {

    val result = Main.add(2, 3)
    assert(result === 5)
  }
}
