package distsort


import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;

import protos.distsort.{
  DistsortGrpc, 
  ReadyRequest,
  ReadyReply,
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
    val port = 50055
    val client = DistSortClient(host, port)
    try {
      val user = "Worker" + args.headOption.getOrElse("1")
      var syncPointOne = client.ready(user)
      
      if (syncPointOne == true) {
        println("Sync Point 1 passed")
      }

    } finally {
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
    logger.info(name + " ready")

    val request = ReadyRequest(workerName = name, ready = true)
    try {
      val response = blockingStub.workerReady(request)
      logger.info("Master: " + response.message)
      return response.finish
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        return false
    }
  }
}