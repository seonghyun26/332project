package network.rpc.worker.client

import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;
import io.grpc.{Server, ServerBuilder};
import scala.concurrent.{ExecutionContext, Future}

import com.google.protobuf.ByteString

import protos.distsortWorker.{
  DistsortWorkerGrpc,
  PartitionRequest,
  PartitionReply,
}

object DistSortClient {
  def apply(host: String, port: Int): DistSortClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = DistsortWorkerGrpc.blockingStub(channel)
    val ipWorker = "localhost"
    val channelWorker = ManagedChannelBuilder.forAddress(ipWorker, 50072).usePlaintext().build
    val blockingStubWorker = DistsortWorkerGrpc.blockingStub(channelWorker)
    new DistSortClient(channel, blockingStub, List((channelWorker, blockingStubWorker)))
  }
}

class DistSortClient (
  private val channel: ManagedChannel,
  private val blockingStub: DistsortWorkerGrpc.DistsortWorkerBlockingStub,
  private val stubList: List[(ManagedChannel, DistsortWorkerGrpc.DistsortWorkerBlockingStub)] 
) {
  private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendPartition(workerName: String, sendToIp: String, data: List[ByteString]): Boolean = {
    logger.info(workerName + " sending partition")

    val request = PartitionRequest(
      workerIpAddress = sendToIp,
      data = data
    )
    // NOTE: Find stub using sendToIp
    // From stub, ip can be achieved by stublist(i)._2.authority()
    val stubFiltered = stubList.filter(_._1.authority() == sendToIp)
    assert( stubFiltered != None)
    val stub = stubFiltered(0)._2

    try {
      val response = stub.partition(request)
      if (response.finish) {
        println(" >> Master : Received partition from "+ sendToIp )
        return true
      }
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client sendPartition")
    }

    return false
  }
}
