package master.server

import network.rpc.master.server.DistSortServer
import java.net.InetAddress
import java.util.logging.Logger
import java.util.concurrent.CountDownLatch
import scala.concurrent.{Promise, SyncVar, Future, Await, ExecutionContext, Channel, blocking}
import scala.concurrent.duration.Duration
import master.util.sync.SyncAccList
import master.Master
import master.server.interceptor.ServerInterceptor


object DistSortServerImpl {
  final val defaultPort = 55555
  implicit val ec = ExecutionContext.global

  private def printConnectedWorkers(workers: List[String]) = {
    println(workers.mkString(", "))
  }
  
  def serveRPC(numWorkers: Int, overridePort: Option[Int]) = {
    val connectedWorkers = Promise[List[String]]
    val receiveFromInterceptor = new Channel[String]
    val port = overridePort.getOrElse(this.defaultPort)
    val server = new DistSortServerImpl(port, numWorkers, connectedWorkers, receiveFromInterceptor)
    val interceptor = new ServerInterceptor(receiveFromInterceptor)
    server.start(Some(interceptor))
    val localIpAddress = InetAddress.getLocalHost.getHostAddress
    println(s"$localIpAddress:$port")
    connectedWorkers.future.foreach(printConnectedWorkers)
    server.blockUntilShutdown()
  }
}

class DistSortServerImpl(port: Int, numWorkers: Int, connectedWorkers: Promise[List[String]], receiveFromInterceptor: Channel[String])
extends DistSortServer(port, ExecutionContext.global) {
  private val logger = Logger.getLogger(classOf[DistSortServerImpl].getName)
  implicit private val ec = ExecutionContext.global

  private val master = new Master(numWorkers)

  private val readyRequestLatch = new CountDownLatch(numWorkers)
  private val keyRangeRequestLatch = new CountDownLatch(numWorkers)
  private val partitionCompleteRequestLatch = new CountDownLatch(numWorkers)
  private val exchangeCompleteRequestLatch = new CountDownLatch(numWorkers)
  private val shutdownLatch = new CountDownLatch(numWorkers)

  private val syncConnectedWorkers = new SyncAccList[String](List())

  private val triggerShutdown = Promise[Unit]
  triggerShutdown.future.foreach{_ => Future { blocking {
      Thread.sleep(3000)
      logger.info("Shutting down the server...")
      this.stop()
      this.blockUntilShutdown()
      logger.info("Server shut down!")
    }
  }}

  def handleReadyRequest(workerName: String, _workerIpAddress: String) = {
    val workerIpAddress = receiveFromInterceptor.read
    syncConnectedWorkers.accumulate(List(workerIpAddress))
    readyRequestLatch.countDown()
    logger.fine("Countdown on readyRequestLatch")
    readyRequestLatch.await()
    connectedWorkers.trySuccess(syncConnectedWorkers.get)
  }

  def handleKeyRangeRequest(
    numSamples: Int,
    samples: List[Array[Byte]],
  ): (List[Array[Byte]], List[String]) = {
    val keyRangeResult = master.divideKeyRange(numSamples, samples, syncConnectedWorkers.get)
    keyRangeRequestLatch.countDown()
    logger.fine("Countdown on keyRangeRequestLatch")
    keyRangeRequestLatch.await()
    val result = keyRangeResult.get
    logger.info("Got key range, returning to worker.")
    result
  }

  def handlePartitionCompleteRequest() = {
    partitionCompleteRequestLatch.countDown()
    logger.fine("Countdown on partitionCompleteRequestLatch")
    partitionCompleteRequestLatch.await()
  }

  def handleExchangeCompleteRequest() = {
    exchangeCompleteRequestLatch.countDown()
    logger.fine("Countdown on exchangeCompleteRequestLatch")
    exchangeCompleteRequestLatch.await()
  }

  def handleSortFinishRequest() = {
    shutdownLatch.countDown()
    logger.fine("Countdown on shutdownLatch")
    shutdownLatch.await()
    triggerShutdown.trySuccess(())
  }
}
