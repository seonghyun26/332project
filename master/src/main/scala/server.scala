package master.server

import network.rpc.master.server.DistSortServer
import scala.concurrent.{ExecutionContext, Promise}
import java.util.concurrent.CountDownLatch
import scala.concurrent.{Promise, SyncVar, Future, Await}
import scala.concurrent.duration.Duration
import master.Master


object DistSortServerImpl {
  final val port = 55555
  implicit val ec = ExecutionContext.global

  private def printConnectedWorkers(workers: List[String]) = {
    println(workers.mkString(", "))
  }
  
  def serveRPC(numWorkers: Int) = {
    val connectedWorkers = Promise[List[String]]
    val server = new DistSortServerImpl(port, numWorkers, connectedWorkers)
    server.start()
    println(server.getListenAddress())
    connectedWorkers.future.foreach(printConnectedWorkers)
    server.blockUntilShutdown()
  }
}

class DistSortServerImpl(port: Int, numWorkers: Int,
  connectedWorkers: Promise[List[String]],
) extends DistSortServer(ExecutionContext.global) {
  private val readyRequestLatch = new CountDownLatch(numWorkers)
  private val keyRangeRequestLatch = new CountDownLatch(numWorkers)
  private val partitionRequestLatch = new CountDownLatch(numWorkers)
  private val syncConnectedWorkers = new SyncVar[List[String]]
  syncConnectedWorkers.put(List())
  private val syncShutdown = new SyncVar[Int]
  syncShutdown.put(numWorkers)

  def handleReadyRequest(workerName: String) = {
    readyRequestLatch.countDown()
    readyRequestLatch.await()
  }

  def handleKeyRangeRequest(
      populationSize: Int,
      numSamples: Int,
      samples: Iterable[Iterable[Byte]],
    ): (String, (List[Byte], List[Byte])) = {
    val allKeyRanges = Master.divideKeyRange(populationSize, numSamples, samples)
    keyRangeRequestLatch.countDown()
    keyRangeRequestLatch.await()
    Await.result(allKeyRanges, Duration.Inf)
  }

  def handlePartitionRequest() = {
    partitionRequestLatch.countDown()
    partitionRequestLatch.await()
  }

  def handleSortFinishRequest() = {
    val count = syncShutdown.take()
    assert(count > 0)
    count match {
      case 1 => this.stop()
      case n => syncShutdown.put(n - 1)
    }
  }
}