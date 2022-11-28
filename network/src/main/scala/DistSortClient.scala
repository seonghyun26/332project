package network.distsort


import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;

import com.google.protobuf.ByteString

import protos.distsort.{
  DistsortGrpc, 
  ReadyRequest,
  ReadyReply,
  KeyRangeRequest,
  KeyRangeReply,
  PartitionRequest,
  PartitionReply,
  SortFinishRequest,
  SortFinishReply
}


object DistSortClient {
  def apply(host: String, port: Int): DistSortClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = DistsortGrpc.blockingStub(channel)
    new DistSortClient(channel, blockingStub)
  }

  // NOTE: Main code what client does
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 50058
    val client = DistSortClient(host, port)
    val samples: List[ByteString] = List(
      ByteString.copyFromUtf8("b"),
      ByteString.copyFromUtf8("c"),
      ByteString.copyFromUtf8("e"),
      ByteString.copyFromUtf8("t")
    )

    try {
      val worker = "Worker" + args.headOption.getOrElse("1")
      var syncPointOne = client.ready(worker)
      if (syncPointOne) {
        println("Sync Point 1 passed\n")
      }

      var syncPointTwo = client.sendKeyRange(worker, samples)
      if (syncPointTwo) {
        println("Sync Point 2 passed\n")
      }

    } finally {
      println("Worker finished!")
      client.shutdown()
    }
  }
}

class DistSortClient private(
  private val channel: ManagedChannel,
  private val blockingStub: DistsortGrpc.DistsortBlockingStub
) {
  private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def ready(name: String): Boolean = {
    logger.info(name + " is ready")

    val request = ReadyRequest(workerName = name)
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

  def sendKeyRange(name: String, samples: List[ByteString]): Boolean = {
    logger.info(name + " sending key range")

    

    val request = KeyRangeRequest(populationSize = 10, numSamples = 4, samples = samples)
    try {
      val responses = blockingStub.keyRange(request)
      for(reply <- responses){
        println(" >> Master: Key range at '" + reply.workerIpAddress + "'" + ": " + reply.lowerBound.toString("UTF-8") + ", " + reply.upperBound.toString("UTF-8"))
      }
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client keyRange")
        return false
    }

    return true
  }
}