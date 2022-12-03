import org.scalatest.funsuite.AnyFunSuite

import common.Key


class KeySuite extends AnyFunSuite {
  test("Mimimum Key Test") {
    val rhs = List(
      List(0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
      List(0, 0, 0, 0, 0, 0, 0, 0, 0, 255),
      List(1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(255, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
      List(255, 255, 255, 255, 255, 255, 255, 255, 255, 255),
      List(0, 183, 68, 85, 217, 155, 129, 67, 56, 124),
      List(107, 19, 72, 32, 120, 19, 57, 40, 211, 183),
    )
    for (r <- rhs) {
      assert(Key.MIN < Key(r.map(_.toByte)))
    }
    assert(Key.MIN < Key.MAX)
    assert(Key.MIN == Key.MIN)
  }

  test("Maximum Key Test") {
    val rhs = List(
      List(255, 255, 255, 255, 255, 255, 255, 255, 255, 254),
      List(254, 255, 255, 255, 255, 255, 255, 255, 255, 255),
      List(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
      List(0, 0, 0, 0, 0, 0, 0, 0, 0, 255),
      List(1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(255, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(254, 0, 0, 0, 0, 0, 0, 0, 0, 0),
      List(0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
      List(0, 183, 68, 85, 217, 155, 129, 67, 56, 124),
      List(107, 19, 72, 32, 120, 19, 57, 40, 211, 183),
    )
    for (r <- rhs) {
      assert(Key(r.map(_.toByte)) < Key.MAX)
    }
    assert(Key.MIN < Key.MAX)
    assert(Key.MAX == Key.MAX)
  }
}