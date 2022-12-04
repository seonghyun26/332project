package it.rpc.master.client

import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import scala.util.Random

import com.google.protobuf.ByteString
import protos.distsortMaster.{
  DistsortMasterGrpc, 
  ReadyRequest,
  ReadyReply,
  KeyRangeRequest,
  KeyRangeReply,
  PartitionCompleteRequest,
  PartitionCompleteReply,
  ExchangeCompleteRequest,
  ExchangeCompleteReply,
  SortFinishRequest,
  SortFinishReply
}

class DummyWorker (workerName: String, port: Int, masterHostname: String, masterPort: Int) {
  private val logger = Logger.getLogger(classOf[DummyWorker].getName)
  val channel = ManagedChannelBuilder.forAddress(masterHostname, masterPort).usePlaintext().build
  val blockingStub = DistsortMasterGrpc.blockingStub(channel)

  def run(): Unit = {
    val syncPointOne = sendReadySignal(workerName, "DummyIpAddress")
    assert(syncPointOne)
    logger.info("Sync Point 1 passed\n")

    // Do sampling
    val numSamples = 5
    var randomSample = Array.fill[Byte](10 * numSamples)(0)
    Random.nextBytes(randomSample)
    val samples: List[ByteString] =
      for (i <- (0 until numSamples).toList) yield ByteString.copyFrom(randomSample.drop(i * 10).take(10))
    assert(samples.length == numSamples)
    // Done sampling
    val (keyList, workerIpList) = sendKeyRange(workerName, samples.length, samples)
    assert(keyList != List.empty)
    assert(workerIpList != List.empty)
    val keyListInString = keyList.map(_.toByteArray.map(_.toChar).mkString)
    logger.info("Sync Point 2 passed\n")

    // Do sorting
    // ...
    // Done sorting
    val syncPointThree = sendPartitionCompleteSignal(workerName)
    assert(syncPointThree)
    logger.info("Sync Point 3 passed\n")

    // Do partition exchange
    // ...
    // Done partition exchange
    val syncPointFour = sendExchangeCompleteSignal(workerName)
    assert(syncPointFour)
    logger.info("Sync Point 4 passed\n")

    // Do local partition merge
    // ...
    // Done local partition merge
    val syncPointFive = sendFinishSignal(workerName)
    assert(syncPointFive)
    logger.info("Sync Point 5 passed\n")

    logger.info(s"$workerName finished!")
    shutdown()
  }


  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendReadySignal(workerName: String, workerIpAddress: String): Boolean = {
    logger.info(workerName + " is ready")
    val request = ReadyRequest(workerName = workerName, workerIpAddress = workerIpAddress)

    try {
      val response = blockingStub.workerReady(request)
      logger.info(" >> Master: All workers are ready!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client ready")
        return false
    }

    return true
  }

  def sendKeyRange(workerName: String, numSamples: Int, samples: List[ByteString]): (List[ByteString], List[String]) = {
    logger.info(workerName + " sending key range")
    val request = KeyRangeRequest( 
      numSamples = numSamples,
      samples = samples
    )

    try {
      val response = blockingStub.keyRange(request)
      logger.info(" >> Master: Sent key range to workers!")
      return (response.keyList.toList, response.workerIpList.toList)
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client keyRange")
    }

    return (List(), List())
  }

  def sendPartitionCompleteSignal(workerName: String): Boolean = {
    logger.info(s"$workerName finished partitioning")
    val request = PartitionCompleteRequest()

    try {
      val response = blockingStub.partitionComplete(request)
      logger.info(" >> Master: All workers have completed partitioning!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client partitionComplete")
        return false
    }

    return true
  }

  def sendExchangeCompleteSignal(workerName: String): Boolean = {
    logger.info(s"$workerName partition exchange complete")
    val request = ExchangeCompleteRequest()

    try {
      val response = blockingStub.exchangeComplete(request)
      logger.info(" >> Master: All workers have completed partition exchange!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client exchangeComplete")
        return false
    }

    return true
  }

  def sendFinishSignal(workerName: String): Boolean ={
    logger.info(workerName + " sending finish signal")
    val request = SortFinishRequest()

    try {
      val response = blockingStub.sortFinish(request)
      logger.info(" >> Master: All workers finished sorting!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client sort finish")
        return false
    }

    return true
  }
}
