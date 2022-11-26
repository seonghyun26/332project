package distsort

import io.grpc.{Server, ServerBuilder};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;

import scala.concurrent.{ExecutionContext, Future}

import protos.distsort.{
  DistsortGrpc, 
  ReadyRequest,
  ReadyReply,
  PartitionRequest,
  PartitionReply,
  SortFinishRequest,
  SortFinishReply
}


object DistSortServer {
  private val logger = Logger.getLogger(classOf[DistSortServer].getName)

  // NOTE: main code where server starts
  def main(args: Array[String]): Unit = {
    val server = new DistSortServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50055
}

class DistSortServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(DistSortServer.port).addService(DistsortGrpc.bindService(new DistsortImpl, executionContext)).build.start
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

  def getReadyMessage(): String = {
    "All Workers ready"
  }

  private class DistsortImpl extends DistsortGrpc.Distsort {
    private var workerNumbers: Int = 2;
    private var readyCnt:Int = 0;

    override def workerReady(req: ReadyRequest) = {
      readyCnt = readyCnt + 1;
      println("Received " + req.workerName + "signal")

      while(readyCnt < workerNumbers){
        Thread.sleep(1000)
        println(LocalDateTime.now() + "," + readyCnt)
      }

      val message = getReadyMessage()
      val reply = ReadyReply(message = message, finish = true)
      Future.successful(reply)
    }

    // override def keyRange(req: Stream[KeyRangeRequest]) = {
    //   val reply = KeyRangeReply()
    //   Future.successful(reply)
    // }

    override def partition(req: PartitionRequest) = {
      val reply = PartitionReply()
      Future.successful(reply)
    }

    override def sortFinish(req: SortFinishRequest) = {
      val reply = SortFinishReply(finish=true)
      Future.successful(reply)
    }
  }

}