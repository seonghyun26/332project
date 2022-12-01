package master

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future, SyncVar, Promise}
import common.Key

class Master(numWorkers: Int) {
  type Bytes = Array[Byte]
  private implicit val ec = ExecutionContext.global

  private val syncNumSamples = new SyncVar[Int]
  private val syncSamples = new SyncVar[List[Bytes]]

  private val remainingRequests = new SyncVar[Int]
  remainingRequests.put(numWorkers)
  private val calculationTrigger = Promise[Unit]
  calculationTrigger.future.foreach(_ => calculateKeyRange())

  private val syncKeyRange = new SyncVar[(List[Array[Byte]], List[String])]

  private var workerIpAddressList: Option[List[String]] = None

  def setWorkerIpAddressList(workerIpAddressList: List[String]) = {
    assert(numWorkers == workerIpAddressList.length)
    assert(workerIpAddressList.isEmpty)
    this.workerIpAddressList = Some(workerIpAddressList)
  }

  def divideKeyRange(
    numSamples: Int,
    samples: List[Bytes],
  ): SyncVar[(List[Bytes], List[String])] = {
    for (sample <- samples) assert(sample.length == 10)
    assert(samples.size == numSamples)

    syncNumSamples.put(numSamples + syncNumSamples.get)
    syncSamples.put(samples ++ syncSamples.get)
    remainingRequests.take match {
      case 1 => calculationTrigger.success(())
      case n => remainingRequests.put(n - 1)
    }
    syncKeyRange
  }

  // Executes only once.
  private def calculateKeyRange() = {
    val numSamples = syncNumSamples.take
    val samples = syncSamples.take
    assert(numSamples == samples.size)
    
    val keys = samples.map(sample => Key(sample.toList))
    val sortedKeys = keys.sort
    assert(sortedKeys.isSorted)

    val keyRange = for {
      i <- (0 until numWorkers).toList
      step <- Some(numSamples / numWorkers)
      index <- Some(i * step)
    } yield sortedKeys(index).asBytes

    val keyRangeResult = keyRange match {
      case head::tail => List[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0).toArray :: tail
      case Nil => Nil
    }

    assert(workerIpAddressList.isDefined && workerIpAddressList.get.size == numWorkers)
    syncKeyRange.put((keyRangeResult, workerIpAddressList.get))
  }
}
