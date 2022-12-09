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
  def apply(workerList: List[String]): DistSortClient = {
    val channelStubList = workerList map { ip => {
      var channel = ManagedChannelBuilder.forAddress(ip, 50050).usePlaintext().build
      var stub = DistsortWorkerGrpc.blockingStub(channel)
      (channel, stub)
    }}
    new DistSortClient(channelStubList)
  }
}

class DistSortClient (
  private val channelStubList: List[(ManagedChannel, DistsortWorkerGrpc.DistsortWorkerBlockingStub)] 
) {
  private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

  def shutdown(): Unit = {
    for ( channel <- channelStubList )
    channel._1.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendPartition(workerName: String, sendToIp: String, data: List[ByteString]): Boolean = {
    logger.info(workerName + " sending partition")

    val request = PartitionRequest(
      workerIpAddress = sendToIp,
      data = data
    )
    // NOTE: Find stub using sendToIp
    // From stub, ip can be achieved by channelStublist(i)._2.authority()
    val stubFiltered = channelStubList.filter(_._1.authority() == sendToIp+":50050")
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
