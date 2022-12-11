package network.rpc.worker.server

import io.grpc.{Server, ServerBuilder};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString

import protos.distsortWorker.{
  DistsortWorkerGrpc,
  PartitionRequest,
  PartitionReply,
}

object DistSortServer {
  private val logger = Logger.getLogger(classOf[DistSortServer].getName)
  private val port = 50050

  // NOTE: main code where server starts
  def main(args: Array[String]): Unit = {
    val server = new DistSortServer(ExecutionContext.global)
    server.start(port)
    server.blockUntilShutdown()
  }
}

class DistSortServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null
  private val logger = Logger.getLogger(classOf[DistSortServer].getName)

  def start(bindingPort: Int): Unit = {
    server = ServerBuilder.forPort(bindingPort).maxInboundMessageSize(100 * 1024 * 1024).addService(DistsortWorkerGrpc.bindService(new DistsortImpl, executionContext)).build.start
    DistSortServer.logger.fine("Server started, listening on " + bindingPort)
    sys.addShutdownHook {
      logger.fine("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      logger.fine("*** server shut down")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  def handlePartition(receivedData: List[ByteString]): Unit = ()

  private class DistsortImpl extends DistsortWorkerGrpc.DistsortWorker {
    private[this] val logger = Logger.getLogger(classOf[DistSortServer].getName)

    override def partition(req: PartitionRequest) = {
      val receiverIp = req.workerIpAddress
      val receivedData = req.data.toList

      // NOTE : save receivedData in machine, receivedData: List[ByteString]
      handlePartition(receivedData)

      val reply = PartitionReply()
      Future.successful(reply)
    }
  }
}
