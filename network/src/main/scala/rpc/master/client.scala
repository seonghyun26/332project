package network.rpc.master.client

import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;
import io.grpc.{Server, ServerBuilder};
import scala.concurrent.{ExecutionContext, Future}

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

object DistSortClient {
  def apply(host: String, port: Int): DistSortClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = DistsortMasterGrpc.blockingStub(channel)
    new DistSortClient(channel, blockingStub)
  }
}

class DistSortClient private(
  private val channel: ManagedChannel,
  private val blockingStub: DistsortMasterGrpc.DistsortMasterBlockingStub,
) {
  private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendReadySignal(workerName: String, workerRpcPort: Int): Boolean = {
    logger.info(workerName + " is ready")
    val request = ReadyRequest(workerName = workerName, workerRpcPort = workerRpcPort)

    try {
      val response = blockingStub.workerReady(request)
      println(" >> Master: All workers are ready!")
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
      println(" >> Master: Sent key range to workers!")
      return (response.keyList.toList, response.workerIpList.toList)
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client keyRange")
    }

    return (List(), List())
  }

  def partitionComplete(workerName: String): Boolean = {
    logger.info(workerName + " partitioning completed")

    val request = PartitionCompleteRequest()

    // NOTE: Find stub using sendToIp
    // From stub, ip can be achieved by stublist(i)._2.authority()

    try {
      val response = blockingStub.partitionComplete(request)
      println(" >> Master: All workers complete partitioning!")
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client partitionComplete")
        return false
    }

    return true
  }

  def exchangeComplete(workerName: String): Boolean = {
    logger.info(workerName + " exchange completed")

    val request = ExchangeCompleteRequest()

    try {
      val response = blockingStub.exchangeComplete(request)
      println(" >> Master: All workers complete exchange!")
    } catch {
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
      println(" >> Master: All workers finished sorting!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client sort finish")
        return false
    }

    return true
  }
}