package worker

import scala.io.Source
import scala.util.Random

import common._


class Block(filepath: String){
  val fileName: String = filepath.split("/").last
  val dir: String = filepath.split("/").dropRight(1).mkString("/")

  var tempDir: String = filepath
  var keyRange: Option[(Int, Int)] = None
  var fileSize: Option[Int] = None

  var numTuples: Option[Int] = None

  var buf: Array[Tuple] = Array()

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
    toStream.toList
  }

  def sorted: List[Tuple] = {
    toList.sortWith{(a: Tuple, b: Tuple) => a < b}
  }

  def sample(sample_size: Int): List[Tuple] = {
    val rand = new Random()
    val population = toList
    val sample_idx = (1 to sample_size).map{ _ => rand.nextInt(population.length) }
    sample_idx.map { idx => population(idx) }.toList
  }

  def divideByPartition(partition: List[Int]): List[Block] = ???

  def sortThenSaveTo(dst: Block): Unit = ???
}