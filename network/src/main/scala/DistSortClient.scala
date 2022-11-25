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

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 50054
    val client = DistSortClient(host, port)
    try {
      val user = args.headOption.getOrElse("world")
      client.ready(user)
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

  /** Say hello to server. */
  def ready(name: String): Unit = {
    logger.info("Worker #1 ready")
    val request = ReadyRequest(ready = true)
    try {
      val response = blockingStub.workerReady(request)
      logger.info("Master: " + response.message)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}