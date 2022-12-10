package worker

import scala.io.Source
import scala.util.Random

import java.lang.IllegalArgumentException

import java.util.logging.Level
import java.util.logging.Logger

import java.io._
import java.net._

import common._

object Block {
  def maxSize = 360000

  private val logger = Logger.getLogger("Block")
  logger.setLevel(Level.INFO)

  def divideTuplesByPartition(
    tuples: List[Tuple], 
    partition: List[Key]
    ): Map[Int, List[Tuple]] = {

    def getPartitionIdxOf(value: Key) = partition.getRangeIdx(value)

    tuples groupBy {t => getPartitionIdxOf(t.key)}
  }

  def fromTuples(path: String, tuples: Iterable[Tuple]): Block = {

    val file = new File(path)
    val target = new BufferedOutputStream( new FileOutputStream(file) )

    tuples.foreach { tuple => target.write(tuple.toBytes.toArray) }
    target.close()

    new Block(path)
  }

  def save(tempDir: String, tuples: Iterable[Tuple]): List[Block] = {

    def rec(tuples: Iterable[Tuple], idx: Int, acc: List[Block]): List[Block] = {
      if (tuples.isEmpty) acc
      else {
        val (frontTuples, left) = tuples splitAt maxSize
        rec(left, idx + 1, fromTuples(tempDir + "/partition." + idx, frontTuples) :: acc)
      }
    }

    rec(tuples, 0, List())
  }
}


class Block(filepath: String){

  val file = new File(filepath)

  require { file.exists() }

  val fileName: String = filepath.split("/").last
  val dir: String = filepath.split("/").dropRight(1).mkString("/")

  def fileSize = file.length.toInt

  require { fileSize % 100 == 0 }

  def numTuples: Int = fileSize / 100

  require { numTuples <= Block.maxSize }

  var partitionIdx: Option[Int] = None

  def setPartitionIdx(idx: Int) = { partitionIdx = Some(idx) }

  def toStream: Stream[Tuple] = {
    val source = Source.fromFile(filepath, "ISO8859-1")

    def stream: Stream[Tuple] = {
      if(!source.hasNext) Stream.empty
      else {
        def byteList = source.take(100).toList.map {_.toByte}
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
    def sortedTuples = sorted
    writeBytes(sortedTuples.toBytes, file)
  }
}