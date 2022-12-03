
package worker


import sys.process._
import scala.util.Random
import java.io.File

import common._

import com.google.protobuf.ByteString


class Worker(val inputDirs: List[String], val outputDir: String) {

  val fileList = inputDirs flatMap {dir => getListOfFiles(dir)}
  val fileNameList = fileList map { file => file.getPath }

  def initialize: List[Block] = { 
    initializeBlocks(fileNameList)
  }

  def sample(blocks: List[Block]): List[ByteString] = {
    val numTotalTuples = blocks.foldLeft(0){ (acc, block) => acc + block.numTuples }
    val sampleSize = if (numTotalTuples < 1000) numTotalTuples else 1000
    val sample = sampleFromBlocks(blocks, sampleSize)

    sample map { tuple => ByteString.copyFrom(tuple.toBytes.toArray) } 
  }

  def partition(blocks: List[Block], keyRange: List[Key]): List[Block] = {
    val newBlokcs = for ( block <- blocks ) yield {
      block.divideByPartition(keyRange)
    }
    newBlokcs.flatten
  }

  def initializeBlocks(fileNameList: List[String]): List[Block] = {
    val blocks = fileNameList map { fileName:String => new Block(fileName) }

    setUpTempDirectory(blocks)

    blocks
  }

  def setUpTempDirectory(blocks: List[Block]): Unit = {

    s"mkdir $outputDir/temp" !

    blocks.zipWithIndex foreach {
      case (block, id) => {

        val tempDir = s"$outputDir/temp/$id/"

        s"mkdir $tempDir" !

        block.setTempDir(tempDir)
      }
    }
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
    Block.save(outputDir, mergedTupleStream)
  }

}