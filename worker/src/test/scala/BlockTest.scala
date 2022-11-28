package test

import java.lang.IllegalArgumentException
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import worker.Tuple
import worker.Block
import test.Partition.loadPartition


class BlockSuite extends AnyFunSuite {
  loadPartition()

  val block1 = new Block("./temp/partition1")


  test("Block Load Test") {
    assert(block1.toList != List())
  }

  test("Block Sort Test") {
    assert(Tuple.sortCheck(block1.sorted))
  }

  test("Block Sample Test") {
    assert(block1.sample(20).length == 20)
  }
}