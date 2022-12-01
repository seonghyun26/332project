
package test

import java.lang.IllegalArgumentException
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import sys.process._

import worker.Tuple
import worker.Block
import worker.Worker

import test.util._


class WorkerSuite extends AnyFunSuite {

  buildGenSort()

  makeDir("./temp")

  makeDir("./temp/input")
  makeDir("./temp/output")

  test("Block Make Test") {
    makeBlock("./temp/input/p1", 1000)
    makeBlock("./temp/input/p2", 1000)
    makeBlock("./temp/input/p3", 1000)
  }

  test("Block Sorting Test") {
    val b1 = new Block("./temp/input/p1")
    val b2 = new Block("./temp/input/p2")
    val b3 = new Block("./temp/input/p3")

    assert {
      b1.sorted.isSorted &&
      b2.sorted.isSorted &&
      b3.sorted.isSorted
    }
  }

  test("Block SortThenSave Test") {
    val b1 = new Block("./temp/input/p1")
    val b2 = new Block("./temp/input/p2")
    val b3 = new Block("./temp/input/p3")

    val d1 = b1.sortThenSaveTo("./temp/output/p1")
    val d2 = b2.sortThenSaveTo("./temp/output/p2")
    val d3 = b3.sortThenSaveTo("./temp/output/p3")

    assert { 
      d1.toList.isSorted &&
      d2.toList.isSorted &&
      d3.toList.isSorted
    }
  }

  test("Worker Merge Test") {

    val d1 = new Block("./temp/output/p1")
    val d2 = new Block("./temp/output/p2")
    val d3 = new Block("./temp/output/p3")

    val blocks = Worker.merge(List(d1, d2, d3))
    val block = blocks(0)

  }
}