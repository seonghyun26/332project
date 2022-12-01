
package worker


import sys.process._
import scala.util.Random


object Worker {

  val outputDir = "./temp/output"

  def main(args: Array[String]): Unit = {
    ???
  }

  def initializeBlocks(fileNameList: List[String]): List[Block] = {
    val blocks = fileNameList map { fileName:String => new Block(fileName) }

    setUpTempDirectory(blocks)

    blocks
  }

  def setUpTempDirectory(blocks: List[Block]): Unit = {
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