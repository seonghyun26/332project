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

  // NOTE: main code where server starts
  def main(args: Array[String]): Unit = {
    val server = new DistSortServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50060
}

class DistSortServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(DistSortServer.port).addService(DistsortWorkerGrpc.bindService(new DistsortImpl, executionContext)).build.start
    DistSortServer.logger.info("Server started, listening on " + DistSortServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class DistsortImpl extends DistsortWorkerGrpc.DistsortWorker {
    private[this] val logger = Logger.getLogger(classOf[DistSortServer].getName)

    override def partition(req: PartitionRequest) = {
      val reply = PartitionReply()
      Future.successful(reply)
    }
  }
}
