import org.scalatest.funsuite.AnyFunSuite

import master.util.sync.{SyncAccList, SyncAccInt}

class SyncAccSuite extends AnyFunSuite {

  test("SyncAccList[T] - mere merge") {
    val syncAcc = new SyncAccList[Int](List(1, 2, 3))
    val incoming = List(4, 5, 6)
    val expected = List(1, 2, 3, 4, 5, 6)
    syncAcc.accumulate(incoming)
    val actual = syncAcc.take
    assert(actual == expected)
  }

  test("SyncAccList[T] - merge with empty") {
    val syncAcc = new SyncAccList[Int](List())
    val incoming = List(4, 5, 6)
    val expected = List(4, 5, 6)
    syncAcc.accumulate(incoming)
    val actual = syncAcc.take
    assert(actual == expected)
  }

  test("SyncAccList[T] - accumulate with empty") {
    val syncAcc = new SyncAccList[Int](List(1, 2, 3))
    val incoming = List()
    val expected = List(1, 2, 3)
    syncAcc.accumulate(incoming)
    val actual = syncAcc.take
    assert(actual == expected)
  }

  test("SyncAccInt - mere merge") {
    val syncAcc = new SyncAccInt(123)
    val incoming = 456
    val expected = 123 + 456
    syncAcc.accumulate(incoming)
    val actual = syncAcc.take
    assert(actual == expected)
  }

  test("SyncAccInt - mere merge negative number") {
    val syncAcc = new SyncAccInt(123)
    val incoming = -456
    val expected = 123 - 456
    syncAcc.accumulate(incoming)
    val actual = syncAcc.take
    assert(actual == expected)
  }
}