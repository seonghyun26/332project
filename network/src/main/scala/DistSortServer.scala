package network.distsort

import io.grpc.{Server, ServerBuilder};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;

import scala.concurrent.{ExecutionContext, Future, Promise}
// import java.util.concurrent.atomic.AtomicInteger
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


object DistSortServer {
  private val logger = Logger.getLogger(classOf[DistSortServer].getName)

  // NOTE: main code where server starts
  def main(args: Array[String]): Unit = {
    val server = new DistSortServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50058
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
    private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

    private var workerNumbers: Int = 2;
    private var readyCnt:Int = 0;
    private var keyRangeCnt:Int = 0;

    // private val promise = Promise[Unit]
    // private val counter = new AtomicInteger(0)

    // val result: Future[Unit] = promise.future

    // override def onNext(v: KeyRangeReply): Unit = {
    // }

    // override def onError(throwable: Throwable): Unit = {
    //   val _ = promise.tryFailure(throwable)
    // }

    // override def onCompleted(): Unit = {
    //   val _ = promise.tryFailure(new RuntimeException("no more completions"))
    // }

    override def workerReady(req: ReadyRequest) = {
      println("Received " + req.workerName + " ready request")
      readyCnt = readyCnt + 1;

      while(readyCnt < workerNumbers){
        Thread.sleep(1000)
        println(LocalDateTime.now() + "," + readyCnt)
      }

      val message = getReadyMessage()
      val reply = ReadyReply()
      Future.successful(reply)
    }

    override def keyRange(req: KeyRangeRequest, responseObserver: StreamObserver[KeyRangeReply] ): Unit = {
      println("Received KeyRange request")
      keyRangeCnt = keyRangeCnt + 1;

      val populationSize = req.populationSize;
      val numSamples = req.numSamples;
      val samples = req.samples;

      while(keyRangeCnt < workerNumbers){
        Thread.sleep(1000);
        println(LocalDateTime.now() + ", keyRange request received: " + keyRangeCnt);
      }

      // var sampleString  = List[String]();
      // samples.foreach( s => {sampleString = sampleString:::s.toString("UTF-8")})
      val sampleString = samples.foldRight(List[String]()){ (x, acc) => x.toString("UTF-8")::acc}
      println("Reveived '" + sampleString + " (" + numSamples + " samples)' from worker");

      val testLowerBound:ByteString = ByteString.copyFromUtf8("a");
      val testUpperBound:ByteString = ByteString.copyFromUtf8("z");
      val testworkerIpAddress : List[String]= List("localhost1", "localhost2");

      try {
        testworkerIpAddress.foreach(
          ipaddress => responseObserver.onNext(KeyRangeReply(
            lowerBound = testLowerBound,
            upperBound = testUpperBound,
            workerIpAddress = ipaddress
          ))
        )
      } catch {
        case e: InterruptedException =>
          logger.info("RPC failed in server")
      }
      responseObserver.onCompleted();
    }

    override def partition(req: PartitionRequest) = {
      val reply = PartitionReply()
      Future.successful(reply)
    }

    override def sortFinish(req: SortFinishRequest) = {
      val reply = SortFinishReply()
      Future.successful(reply)
    }
  }

}