
package worker


import sys.process._
import scala.util.Random
import java.io.File

import java.util.logging.Level
import java.util.logging.Logger

import common._

import com.google.protobuf.ByteString


class Worker(val inputDirs: List[String], val outputDir: String) {
  private val logger = Logger.getLogger("Worker")
  logger.setLevel(Level.INFO)

  val fileList = inputDirs flatMap {dir => getListOfFiles(dir)}
  val fileNameList = fileList map { file => file.getPath }

  def blocks: List[Block] = { 
    initializeBlocks(fileNameList)
  }

  def sample(blocks: List[Block]): List[ByteString] = {
    val numTotalTuples = blocks.foldLeft(0){ (acc, block) => acc + block.numTuples }
    val sampleSize = if (numTotalTuples < 1000) numTotalTuples else 1000
    val sample = sampleFromBlocks(blocks, sampleSize)

    sample map { tuple => tuple.key.toByteString } 
  }

  def partition(blocks: List[Block], keyRange: List[Key]): List[Block] = {
    for {
      (block, blockId) <- blocks.zipWithIndex
      (workerIndex, tuples) <- block.divideByPartition(keyRange)
    } yield {
      val newBlockPath = outputDir + s"/$blockId.$workerIndex"
      val newBlock = Block.fromTuples(newBlockPath, tuples)
      newBlock.setPartitionIdx(workerIndex)
      newBlock
    }
  }

  def initializeBlocks(fileNameList: List[String]): List[Block] = {
    val blocks = fileNameList map { fileName:String => new Block(fileName) }

    blocks
  }

  def sampleFromBlocks(blockList: List[Block], sampleSize: Int): List[Tuple] = {
    val rand = new Random()

    val sampleLocation = (1 to sampleSize).map { _ => rand.nextInt(blockList.length) }

    val localSampleSize = for { i <- 0 to blockList.length } yield
    {
      sampleLocation.count(_==i)
    }

    val sample = (localSampleSize zip blockList) flatMap { 
      case (size: Int, block: Block) => block.sample(size)
    }

    sample.toList
  }

  def merge(blocks: List[Block]): List[Block] = {
    val streamList: List[Stream[Tuple]] = blocks map { block => block.toStream }
    val mergedTupleStream: Stream[Tuple] = streamList.mergedStream

    println("Start save")

    Block.save(outputDir, mergedTupleStream)
  }

}