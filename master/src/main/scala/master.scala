package master

import java.util.logging.Logger
import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future, SyncVar, Promise}
import common.Key
import util.sync.{SyncAccInt, SyncAccList}

class Master(numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[Master].getName)

  type Bytes = Array[Byte]
  private implicit val ec = ExecutionContext.global

  private val syncNumSamples = new SyncAccInt(0)
  private val syncSamples = new SyncAccList[Bytes](List())

  private val remainingRequests = new CountDownLatch(numWorkers)
  private val calculationTrigger = Promise[Unit]
  calculationTrigger.future.foreach(_ => calculateKeyRange())
  private val syncKeyRange = new SyncVar[(List[Array[Byte]], List[String])]
  private var workerIpAddressList = new SyncVar[List[String]]

  def setWorkerIpAddressList(workerIpAddressList: List[String]) = {
    assert(numWorkers == workerIpAddressList.length)
    this.workerIpAddressList.put(workerIpAddressList)
  }

  def divideKeyRange(
    numSamples: Int,
    samples: List[Bytes],
    workerIpAddressList: List[String],
  ): SyncVar[(List[Bytes], List[String])] = {
    for (sample <- samples) assert(sample.length == 10)
    assert(samples.size == numSamples)

    this.setWorkerIpAddressList(workerIpAddressList)

    syncNumSamples.accumulate(numSamples)
    syncSamples.accumulate(samples)
    remainingRequests.countDown()
    remainingRequests.await()

    calculationTrigger.trySuccess(())
    syncKeyRange
  }

  // This is and should be executed only once.
  private def calculateKeyRange() = {
    val numSamples = syncNumSamples.take
    val samples = syncSamples.take
    assert(numSamples == samples.size)
    
    val keys = samples.map(sample => Key(sample.toList))
    val sortedKeys = keys.sort
    assert(sortedKeys.isSorted)

    val step = numSamples / numWorkers
    val keyRange = for {
      i <- (0 until numWorkers by step).toList
    } yield sortedKeys(i).asBytes

    val minimumKey = Key(List.fill(10)(0.toByte)).asBytes
    val keyRangeResult = keyRange match {
      case head :: tail => minimumKey :: tail
      case Nil => Nil
    }

    if (!this.workerIpAddressList.isSet) {
      logger.warning("Worker IP address list is not set")
    }
    if (workerIpAddressList.get.size != numWorkers) {
      logger.warning("Worker IP address list is not complete")
    }
    assert(workerIpAddressList.isSet && workerIpAddressList.get.size == numWorkers)
    logger.info("Key range calculated")
    syncKeyRange.put((keyRangeResult, workerIpAddressList.get))
  }
}
