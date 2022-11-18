package worker

import scala.io.Source
import scala.util.Random

import common._


class Block(filepath: String){
    val filename: String = filepath.split("/").last
    val dir: String = filepath.split("/").dropRight(1).mkString("/")

    var temp_dir: String = filepath
    var key_range: (Int, Int) = (0, 0)
    var file_size: Int = 0

    var num_tuples: Int = 0

    var buf: Array[Tuple] = Array()

    def toStream: Stream[Tuple] = {
        val source = Source.fromFile(filepath, "ISO8859-1")

        def stream: Stream[Tuple] = {
            if(!source.hasNext) Stream.empty
            else {
                val char_list = source.take(100).toList
                val byte_list = char_list.map {_.toByte}
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