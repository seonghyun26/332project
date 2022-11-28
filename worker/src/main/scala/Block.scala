package worker

import scala.io.Source
import scala.util.Random

import java.lang.IllegalArgumentException

import java.io._
import common._


object Block {
  def maxSize = 360000

  def divideTuplesByPartition(
    tuples: List[Tuple], 
    partition: List[Key]
    ): Map[Int, List[Tuple]] = {

    def getPartitionIdxOf(value: Key) = Key.getRangeIdx(partition)(value)
    tuples groupBy {t => getPartitionIdxOf(t.key)}
  }

  def saveOne(path: String, tuples: List[Tuple]): Block = {
    require(tuples.length <= maxSize)

    // Save the tuples to path

    val byteList = tuples.foldLeft(List[Byte]()){
      (acc: List[Byte], tuple: Tuple) => tuple.toBytes ++ acc
    }

    writeBytes(byteList.toStream, new File(path))

    new Block(path)
  }

  def save(tempDir: String, tuples: List[Tuple]): List[Block] = {

    def rec(tuples: List[Tuple], idx: Int): List[Block] = {
      if (tuples.isEmpty) List()
      else {
        val (frontTuples, left) = tuples splitAt maxSize
        saveOne(tempDir + "/" + idx, frontTuples) :: rec(left, idx + 1)
      }
    }
    rec(tuples, 0)
  }
}


class Block(filepath: String){
  val fileName: String = filepath.split("/").last
  val dir: String = filepath.split("/").dropRight(1).mkString("/")

  var tempDir: Option[String] = None
  var partitionIdx: Option[Int] = None

  var numTuples: Option[Int] = None

  def setTempDir(path: String): Unit = { tempDir = Some(path) }

  def toStream: Stream[Tuple] = {
    val source = Source.fromFile(filepath, "ISO8859-1")

    def stream: Stream[Tuple] = {
    if(!source.hasNext) Stream.empty
      else {
        val byte_list = source.take(100).toList.map {_.toByte}
        Tuple.fromBytes(byte_list) #:: stream
      }
    }
    stream
  }

  def toList: List[Tuple] = {
    val list = toStream.toList
    numTuples = Some(list.length)
    list
  }

  def sorted: List[Tuple] = {
    toList.sortWith{(a: Tuple, b: Tuple) => a < b}
  }

  def sample(sample_size: Int): List[Tuple] = {
    val rand = new Random()
    val population = toList
    val sampleIdx = (1 to sample_size).map{ _ => rand.nextInt(population.length) }
    sampleIdx.map { idx => population(idx) }.toList
  }

  def divideByPartition(partition: List[Key]): List[Block] = {
    require { tempDir != None }

    val dir = tempDir match {
      case Some(dir) => dir
      case None => throw new IllegalArgumentException("Temporary directory must be set.")
    }

    val file = new File(dir)
    file.mkdir()

    val numPartition = partition.length + 1

    val partitioned = Block.divideTuplesByPartition(toList, partition)

    (
    for { (idx, tuples) <- partitioned  }
    yield {

      // Each tuple list are smaller than original,
      // so it can be saved into one block.


      val block = Block.saveOne(dir + "/" + idx, tuples)
      block.partitionIdx = Option(idx)
      block.numTuples = Option(tuples.length)

      block
    }
    ).toList
  }

  def sortThenSaveTo(dst: Block): Unit = {
    
  }
}