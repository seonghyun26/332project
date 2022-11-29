
package worker


object Worker {

  val outputDir = "./output"

  def merge(blocks: List[Block]): List[Block] = {
    val streamList: List[Stream[Tuple]] = blocks map { block => block.toStream }
    val mergedTupleStream: Stream[Tuple] = Tuple.mergeStream(streamList)
    Block.save(outputDir, mergedTupleStream)
  }

}