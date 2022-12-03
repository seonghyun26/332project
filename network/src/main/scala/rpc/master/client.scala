package network.rpc.master.client

import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;
import io.grpc.{Server, ServerBuilder};
import scala.concurrent.{ExecutionContext, Future}

import com.google.protobuf.ByteString

import protos.distsortMaster.{
  DistsortMasterGrpc, 
  ReadyRequest,
  ReadyReply,
  KeyRangeRequest,
  KeyRangeReply,
  SortFinishRequest,
  SortFinishReply
}

// object DistSortClient {
//   def apply(host: String, port: Int): DistSortClient = {
//     val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
//     val blockingStub = DistsortMasterGrpc.blockingStub(channel)
//     val ipWorker = "localhost"
//     val channelWorker = ManagedChannelBuilder.forAddress(ipWorker, 50072).usePlaintext().build
//     val blockingStubWorker = DistsortMasterGrpc.blockingStub(channelWorker)
//     new DistSortClient(channel, blockingStub, List((channelWorker, blockingStubWorker)))
//   }

//   def main(args: Array[String]): Unit = {
//     val host: String = "localhost"
//     val port: Int = 50060
    
//     val client = DistSortClient(host, port)
//     val workerName: String = "Worker" + args.headOption.getOrElse("")
//     val workerIpAddress: String = "localhost"

//     val samples: List[ByteString] = List(
//       ByteString.copyFrom("b".getBytes),
//       ByteString.copyFrom("c".getBytes),
//       ByteString.copyFrom("e".getBytes),
//       ByteString.copyFrom("t".getBytes)
//     )

//     val workerServer = new DistSortWorkerServer(ExecutionContext.global)
    

//     //NOTE: What client does
//     try {
//       workerServer.start()
      
//       var syncPointOne = client.sendReadySignal(workerName, workerIpAddress)
//       if (syncPointOne) {
//         println("Sync Point 1 passed\n")
//       }

//       val (keyList:List[ByteString], workerIpList) = client.sendKeyRange(workerName, 4, samples)
//       val keyListInString = keyList.map(_.toByteArray.map(_.toChar).mkString)
//       println(keyListInString)
//       println(workerIpList)
//       println("Sync Point 2 passed\n")

//       val testDestination: String = "localhost:" + "50072"
//       val syncPointThree = client.sendPartition(
//         workerName,
//         testDestination,
//         samples
//       )
//       if (syncPointThree) {
//         println("Sync Point 3 passed\n")
//       }

//       var syncPointFour = client.sendFinishSignal(workerName)
//       if (syncPointFour) {
//         println("Sync Point 4 passed\n")
//       }
      
//     } finally {
//       println("Worker finished!")
//       client.shutdown()
//       workerServer.stop()
//     }
//   }
// }

class DistSortClient private(
  private val channel: ManagedChannel,
  private val blockingStub: DistsortMasterGrpc.DistsortMasterBlockingStub,
  private val stubList: List[(ManagedChannel, DistsortMasterGrpc.DistsortMasterBlockingStub)] 
) {
  private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendReadySignal(workerName: String, workerIpAddress: String): Boolean = {
    logger.info(workerName + " is ready")
    val request = ReadyRequest(workerName = workerName, workerIpAddress = workerIpAddress)

    try {
      val response = blockingStub.workerReady(request)
      println(" >> Master: All workers are ready!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client ready")
        return false
    }

    return true
  }

  def sendKeyRange(workerName: String, numSamples: Int, samples: List[ByteString]): (List[ByteString], List[String]) = {
    logger.info(workerName + " sending key range")
    val request = KeyRangeRequest( 
      numSamples = numSamples,
      samples = samples
    )

    try {
      val response = blockingStub.keyRange(request)
      println(" >> Master: Sent key range to workers!")
      return (response.keyList.toList, response.workerIpList.toList)
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client keyRange")
    }

    return (List(), List())
  }

  def sendFinishSignal(workerName: String): Boolean ={
    logger.info(workerName + " sending finish signal")
    val request = SortFinishRequest()

    try {
      val response = blockingStub.sortFinish(request)
      println(" >> Master: All workers finished sorting!")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client sort finish")
        return false
    }

    return true
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
//     server = ServerBuilder.forPort(DistSortWorkerServer.port).addService(DistsortMasterGrpc.bindService(new DistsortWorkerImpl, executionContext)).build.start
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

//   private class DistsortWorkerImpl extends DistsortMasterGrpc.DistsortMaster {
//     private[this] val logger = Logger.getLogger(classOf[DistSortWorkerServer].getName)
    
//     override def workerReady(req: ReadyRequest) = {
//       val reply = ReadyReply()
//       Future.successful(reply)
//     }
//     override def keyRange(req: KeyRangeRequest) = {
//       val reply = KeyRangeReply()
//       Future.successful(reply)
//     }
//     override def sortFinish(req: SortFinishRequest) = {
//       val reply = SortFinishReply()
//       Future.successful(reply)
//     }

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
