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

  // def main(args: Array[String]): Unit = {
  //   val host: String = "localhost"
  //   val port: Int = 50060
    
  //   val client = DistSortClient(host, port)
  //   val workerName: String = "Worker" + args.headOption.getOrElse("")
  //   val workerIpAddress: String = "localhost"

  //   val samples: List[ByteString] = List(
  //     ByteString.copyFrom("b".getBytes),
  //     ByteString.copyFrom("c".getBytes),
  //     ByteString.copyFrom("e".getBytes),
  //     ByteString.copyFrom("t".getBytes)
  //   )

  //   val workerServer = new DistSortWorkerServer(ExecutionContext.global)
    

  //   //NOTE: What client does
  //   try {
  //     workerServer.start()
      
  //     var syncPointOne = client.sendReadySignal(workerName, workerIpAddress)
  //     if (syncPointOne) {
  //       println("Sync Point 1 passed\n")
  //     }

  //     val (keyList:List[ByteString], workerIpList) = client.sendKeyRange(workerName, 4, samples)
  //     val keyListInString = keyList.map(_.toByteArray.map(_.toChar).mkString)
  //     println(keyListInString)
  //     println(workerIpList)
  //     println("Sync Point 2 passed\n")

  //     val testDestination: String = "localhost:" + "50072"
  //     val syncPointThree = client.sendPartition(
  //       workerName,
  //       testDestination,
  //       samples
  //     )
  //     if (syncPointThree) {
  //       println("Sync Point 3 passed\n")
  //     }

  //     // var syncPointFour = client.sendFinishSignal(workerName)
  //     // if (syncPointFour) {
  //     //   println("Sync Point 4 passed\n")
  //     // }
      
  //   } finally {
  //     println("Worker finished!")
  //     client.shutdown()
  //     workerServer.stop()
  //   }
  // }
}

class DistSortClient private(
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

// // NOTE: Test server object
// object DistSortWorkerServer {
//   private val logger = Logger.getLogger(classOf[DistSortWorkerServer].getName)

//   // NOTE: main code where server starts
//   def main(args: Array[String]): Unit = {
//     val server = new DistSortWorkerServer(ExecutionContext.global)
//     server.start()
//     server.blockUntilShutdown()
//   }

//   private val port = 50071
// }

// class DistSortWorkerServer(executionContext: ExecutionContext) { self =>
//   private[this] var server: Server = null
//   private[this] var receivedDatas: List[ByteString] = List[ByteString]()
//   private[this] var receivedDataNumber: Int = 0

//   def start(): Unit = {
//     server = ServerBuilder.forPort(DistSortWorkerServer.port).addService(DistsortWorkerGrpc.bindService(new DistsortWorkerImpl, executionContext)).build.start
//     DistSortWorkerServer.logger.info("Server started, listening on " + DistSortWorkerServer.port)
//     sys.addShutdownHook {
//       System.err.println("*** shutting down gRPC server since JVM is shutting down")
//       self.stop()
//       System.err.println("*** server shut down")
//     }
//   }

//   def stop(): Unit = {
//     if (server != null) {
//       server.shutdown()
//     }
//   }

//   def blockUntilShutdown(): Unit = {
//     if (server != null) {
//       server.awaitTermination()
//     }
//   }

//   private class DistsortWorkerImpl extends DistsortWorkerGrpc.DistsortWorker {
//     private[this] val logger = Logger.getLogger(classOf[DistSortWorkerServer].getName)

//     override def partition(req: PartitionRequest) = {
//       val receiverIp = req.workerIpAddress;
//       val receivedData = req.data.toList
//       println("receivedData: " + receivedData)
//       // NOTE: save receivedData in machine, receivedData: List[ByteString]

//       val reply = PartitionReply(finish = true)
//       Future.successful(reply)
//     }
//   }
// }
