package network.rpc.master.server

import io.grpc.{Server, ServerBuilder, ServerInterceptor, ServerInterceptors};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;
import java.util.concurrent.locks.ReentrantReadWriteLock

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

abstract class DistSortServer(port: Int, executionContext: ExecutionContext) {
  private val logger = Logger.getLogger(classOf[DistSortServer].getName)
  private var server: Server = null

  def start(interceptor: Option[ServerInterceptor]): Unit = {
    val service = 
      if (interceptor.isDefined) ServerInterceptors.intercept(DistsortMasterGrpc.bindService(new DistsortImpl, executionContext), interceptor.get)
      else DistsortMasterGrpc.bindService(new DistsortImpl, executionContext)
    server = ServerBuilder
      .forPort(port)
      .addService(service)
      .build()
      .start()
    this.logger.info("Server started, listening on " + port)
    sys.addShutdownHook {
      logger.info("*** shutting down gRPC server since JVM is shutting down")
      this.stop()
      logger.info("*** server shut down")
    }
  }

  def getListenAddress(): String = {
    val socket = server.getListenSockets().get(0)
    socket.toString()
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  def handleReadyRequest(workerName: String, workerRpcPort: Int): Unit

  def handleKeyRangeRequest(
      numSamples: Int,
      samples: List[Array[Byte]],
    ): (List[Array[Byte]], List[String])

  def handlePartitionCompleteRequest(): Unit
    
  def handleExchangeCompleteRequest(): Unit

  def handleSortFinishRequest(): Unit

  private class DistsortImpl extends DistsortMasterGrpc.DistsortMaster {
    override def workerReady(req: ReadyRequest): Future[ReadyReply] = {
      logger.info("Received ready request from " + req.workerName)

      val _ = handleReadyRequest(req.workerName, req.workerRpcPort)
      val reply = ReadyReply()
      Future.successful(reply)
    }

    override def keyRange(req: KeyRangeRequest): Future[KeyRangeReply] = {
      logger.info("Received KeyRange request")

      val numSamples = req.numSamples;
      val samples = req.samples;

      logger.info("Reveived " + numSamples + " samples from worker");
      val sampleString = samples.foldRight(List[Array[Byte]]()){ (x, acc) => x.toByteArray::acc}
      val (keyList_, workerIpAddressList) = handleKeyRangeRequest(numSamples, sampleString)
      val keyList = for (key <- keyList_) yield ByteString.copyFrom(key)

      val reply = KeyRangeReply(
        keyList = keyList,
        workerIpList = workerIpAddressList,
      )
      Future.successful(reply)
    }

    override def partitionComplete(request: PartitionCompleteRequest): Future[PartitionCompleteReply] = {
      logger.info("Received PartitionCompleteRequest")

      val _ = handlePartitionCompleteRequest()
      val reply = PartitionCompleteReply()
      Future.successful(reply)
    }

    override def exchangeComplete(request: ExchangeCompleteRequest): Future[ExchangeCompleteReply] = {
      logger.info("Received ExchangeCompleteRequest")

      val _ = handleExchangeCompleteRequest()
      val reply = ExchangeCompleteReply()
      Future.successful(reply)
    }

    override def sortFinish(req: SortFinishRequest): Future[SortFinishReply] = {
      logger.info("Received sortFinish request")

      val _ = handleSortFinishRequest()
      val reply = SortFinishReply()
      Future.successful(reply)
    }
  }
}