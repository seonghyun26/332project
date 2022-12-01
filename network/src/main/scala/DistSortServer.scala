package network.distsort.server

import io.grpc.{Server, ServerBuilder};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString


import protos.distsort.{
  DistsortGrpc, 
  ReadyRequest,
  ReadyReply,
  KeyRangeRequest,
  KeyRangeReply,
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

  private val port = 50052
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
    private[this] val logger = Logger.getLogger(classOf[DistSortServer].getName)

    private var workerNumbers: Int = 2;
    private var readyCnt: Int = 0;
    private var readyLock = new ReentrantReadWriteLock()
    private var keyRangeCnt: Int = 0;
    private var keyRangeLock = new ReentrantReadWriteLock()
    private var sortFinishCnt: Int = 0;
    private var sortFinishLock = new ReentrantReadWriteLock()

    override def workerReady(req: ReadyRequest) = {
      println("Received ready request from " + req.workerName)
      readyLock.writeLock().lock()
      try { readyCnt += 1 }
      finally { readyLock.writeLock().unlock() }

      while(readyCnt < workerNumbers){
        Thread.sleep(1000)
        println(LocalDateTime.now() + ", ready request received: " + readyCnt);
      }

      val reply = ReadyReply()
      Future.successful(reply)
    }

    override def keyRange(req: KeyRangeRequest) = {
      println("Received KeyRange request")
      keyRangeLock.writeLock().lock()
      try { keyRangeCnt += 1 }
      finally { keyRangeLock.writeLock().unlock() }

      val populationSize = req.populationSize;
      val numSamples = req.numSamples;
      val samples = req.samples;

      while(keyRangeCnt < workerNumbers){
        Thread.sleep(1000);
        println(LocalDateTime.now() + ", keyRange request received: " + keyRangeCnt);
      }

      val sampleString = samples.foldRight(List[Array[Byte]]()){ (x, acc) => x.toByteArray::acc}
      println("Reveived '" + sampleString + " (" + numSamples + " samples)' from worker");
      // Master Algorithm
      val testKeyList: List[ByteString] = List(
        ByteString.copyFrom("a".getBytes),
        ByteString.copyFrom("d".getBytes),
        ByteString.copyFrom("j".getBytes)
      )
      val testWorkerIpAddressList : List[String]= List(
        "localhost1",
        "localhost2",
        "localhost3",
        "localhost4"
      );

      val reply = KeyRangeReply(
        keyList = testKeyList,
        workerIpAddressList = testWorkerIpAddressList
      )
      Future.successful(reply)
    }

    override def sortFinish(req: SortFinishRequest) = {
      println("Received sortFinish request")
      sortFinishLock.writeLock().lock()
      try { sortFinishCnt += 1 }
      finally { sortFinishLock.writeLock().unlock() }

      while(sortFinishCnt < workerNumbers){
        Thread.sleep(1000);
        println(LocalDateTime.now() + ", sortFinish request received: " + sortFinishCnt);
      }

      val reply = SortFinishReply()
      Future.successful(reply)
    }
  }

}