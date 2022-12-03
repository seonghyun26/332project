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

    def getPartitionIdxOf(value: Key) = partition.getRangeIdx(value)

    tuples groupBy {t => getPartitionIdxOf(t.key)}
  }

  def fromTuples(path: String, tuples: List[Tuple]): Block = {
    require(tuples.length <= maxSize)

    writeBytes(tuples.toBytes, new File(path))

    new Block(path)
  }

  def save(tempDir: String, tuples: Iterable[Tuple]): List[Block] = {

    def rec(tuples: Iterable[Tuple], idx: Int): List[Block] = {
      if (tuples.isEmpty) List()
      else {
        val (frontTuples, left) = tuples splitAt maxSize
        fromTuples(tempDir + idx, frontTuples.toList) :: rec(left, idx + 1)
      }
    }

    rec(tuples, 0)
  }
}


class Block(filepath: String){

  val file = new File(filepath)

  require { file.exists() }

  val fileName: String = filepath.split("/").last
  val dir: String = filepath.split("/").dropRight(1).mkString("/")

  def fileSize = file.length.toInt

  require { fileSize < Block.maxSize && fileSize % 100 == 0 }

  val numTuples: Int = fileSize / 100

  var partitionIdx: Option[Int] = None

  def setPartitionIdx(idx: Int) = { partitionIdx = Some(idx) }

  def toStream: Stream[Tuple] = {
    val source = Source.fromFile(filepath, "ISO8859-1")

    def stream: Stream[Tuple] = {
      if(!source.hasNext) Stream.empty
      else {
        val byteList = source.take(100).toList.map {_.toByte}
        Tuple.fromBytes(byteList) #:: stream
      }
    }
    stream
  }

  def toList: List[Tuple] = {
    toStream.toList
  }

  def sorted: List[Tuple] = {
    toList.sort
  }

  def sample(sampleSize: Int): List[Tuple] = {
    val rand = new Random()
    val population = toList
    val sampleIdx = (1 to sampleSize).map{ _ => rand.nextInt(population.length) }
    sampleIdx.map { idx => population(idx) }.toList
  }

  def divideByPartition(partition: List[Key]): Map[Int, List[Tuple]] = {
    Block.divideTuplesByPartition(toList, partition)
  }

  def sortThenSaveTo(dst: String): Block = {
    Block.fromTuples(dst, sorted)
  }

  def sortThenSave: Unit = {
    val sortedTuples = sorted
    writeBytes(sortedTuples.toBytes, file)
  }
}