package master

import scala.concurrent.{ExecutionContext, Future}

object Master {
  private implicit val ec = ExecutionContext.global

  def divideKeyRange(
    populationSize: Int,
    numSamples: Int,
    samples: Iterable[Iterable[Byte]],
    ): Future[(String, (List[Byte], List[Byte]))] = {
    Future {("", (List(), List()))}
  }
}

class Master {}
