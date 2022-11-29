
package worker


import sys.process._


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
    ???
  }


  def merge(blocks: List[Block]): List[Block] = {
    val streamList: List[Stream[Tuple]] = blocks map { block => block.toStream }
    val mergedTupleStream: Stream[Tuple] = streamList.mergedStream
    Block.save(outputDir, mergedTupleStream)
  }

}