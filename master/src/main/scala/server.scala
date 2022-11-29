import network.distsort.server.DistSortServer
import scala.concurrent.ExecutionContext
import java.util.concurrent.CountDownLatch
import scala.concurrent.{Promise, SyncVar}
import master.Master

class DistSortServerImpl(port: Int, numWorkers: Int, notifyShutdown: Promise[Unit]) extends DistSortServer(ExecutionContext.global) {
  private val readyRequestLatch = new CountDownLatch(numWorkers)
  private val keyRangeRequestLatch = new CountDownLatch(numWorkers)
  private val partitionRequestLatch = new CountDownLatch(numWorkers)
  private val shutdownLatch = new SyncVar[Int]
  shutdownLatch.put(numWorkers)

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
    allKeyRanges
  }

  def handlePartitionRequest() = {
    partitionRequestLatch.countDown()
    partitionRequestLatch.await()
  }

  def handleSortFinishRequest() = {
    val count = shutdownLatch.take()
    assert(count > 0)
    count match {
      case 1 => notifyShutdown.success(())
      case n => shutdownLatch.put(n - 1)
    }
  }
}