package test

import java.lang.IllegalArgumentException
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import worker.Tuple
import worker.Block
import test.util._


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

  test("Partition Division Test 1") {
    val sample = block1.sample(19) map {t => t.key}

    val partitioned = Block.divideTuplesByPartition(block1.toList, sample)

    val list = partitioned.toList map {
      case (a: Int, b: List[Tuple]) => b
    }

    assert(partitioned.size <= 20)
    assert(list.flatten.length == block1.toList.length)
  }

  test("Partition Division Test 2") {
    val sample = block1.sample(9) map {t => t.key}
    block1.tempDir = Some("./temp/block1")

    val blocks = block1.divideByPartition(sample)

    for { block <- blocks } yield 
    {
      val partitionIdx = block.partitionIdx match { case Some(i) => i }
      sample(partitionIdx)
    }
  }
}