package network.distsort


import io.grpc.{Channel, StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import java.util.logging.{Level, Logger};
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.util.Iterator;

import com.google.protobuf.ByteString

import protos.distsort.{
  DistsortGrpc, 
  ReadyRequest,
  KeyRangeRequest,
  SortFinishRequest,
}


object DistSortClient {
  def apply(host: String, port: Int): DistSortClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = DistsortGrpc.blockingStub(channel)
    new DistSortClient(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val host: String = "localhost"
    val port: Int = 50052
    
    val client = DistSortClient(host, port)
    val workerName: String = "Worker" + args.headOption.getOrElse("")
    val workerIpAddress: String = "localhost"

    val samples: List[ByteString] = List(
      ByteString.copyFrom("b".getBytes),
      ByteString.copyFrom("c".getBytes),
      ByteString.copyFrom("e".getBytes),
      ByteString.copyFrom("t".getBytes)
    )

    //NOTE: What client does
    try {
      
      var syncPointOne = client.sendReadySignal(workerName, workerIpAddress)
      if (syncPointOne) {
        println("Sync Point 1 passed\n")
      }

      val (keyList, workerIpList) = client.sendKeyRange(workerName, 10, 4, samples)
      val keyListInString = keyList.map(_.toByteArray.map(_.toChar).mkString)
      println(keyListInString)
      println(workerIpList)
      println("Sync Point 2 passed\n")

      // var syncPointThree = client.sendPartition()
      // if (syncPointTwo) {
      //   println("Sync Point 3 passed\n")
      // }

      var syncPointFour = client.sendFinishSignal(workerName)
      if (syncPointFour) {
        println("Sync Point 4 passed\n")
      }
      
    } finally {
      println("Worker finished!")
      client.shutdown()
    }
  }
}

class DistSortClient private(
  private val channel: ManagedChannel,
  private val blockingStub: DistsortGrpc.DistsortBlockingStub
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


  def sendKeyRange(workerName: String, populationSize: Int, numSamples: Int, samples: List[ByteString]): (List[ByteString], List[String]) = {
    logger.info(workerName + " sending key range")
    val request = KeyRangeRequest( 
      populationSize = populationSize,
      numSamples = numSamples,
      samples = samples
    )

    try {
      val response = blockingStub.keyRange(request)
      println(" >> Master: Sent key range to workers!")
      return (response.keyList.toList, response.workerIpAddressList.toList)
    } catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed in client keyRange")
    }

    return (List(), List())
  }

  def sendPartition(workerName: String, data: Iterable[Byte]): Boolean = {
    // TODO: convert byte to bytestring and send
    logger.info(workerName + " sending partition")
    return true
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


// class DistSortWorkerServer(executionContext: ExecutionContext) { self =>
//   private[this] var server: Server = null

//   private def start(): Unit = {
//     server = ServerBuilder.forPort(DistSortWorkerServer.port).addService(DistsortGrpc.bindService(new DistsorWorkertImpl, executionContext)).build.start
//     DistSortWorkerServer.logger.info("Server started, listening on " + DistSortWorkerServer.port)
//     sys.addShutdownHook {
//       System.err.println("*** shutting down gRPC server since JVM is shutting down")
//       self.stop()
//       System.err.println("*** server shut down")
//     }
//   }

//   private def stop(): Unit = {
//     if (server != null) {
//       server.shutdown()
//     }
//   }

//   private def blockUntilShutdown(): Unit = {
//     if (server != null) {
//       server.awaitTermination()
//     }
//   }

//   private class DistsorWorkertImpl extends DistsortGrpc.Distsort {
//     private[this] val logger = Logger.getLogger(classOf[DistSortClient].getName)
//   }

//   override def partition(req: PartitionRequest) = {
//     val receiverIp = req.workerIpAddress;

//     // Find which partition to send
//     val testPartitionToSend:List[ByteString] = List(
//       ByteString.copyFromUtf8("a"),
//       ByteString.copyFromUtf8("z")
//     );

//     val reply = PartitionReply(data = testPartitionToSend)
//     Future.successful(reply)
//   }

// }