
package worker

import sys.process._
import java.io.File

import java.util.logging.Level
import java.util.logging.Logger

import common._

import network.rpc.master.client.{DistSortClient => Master}

import network.rpc.worker.client.{DistSortClient => WorkerClient}
import network.rpc.worker.server.{DistSortServer}

import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext
import java.util.concurrent.locks.ReentrantReadWriteLock

object Entrypoint {
  private val logger = Logger.getLogger("Worker")
  logger.setLevel(Level.INFO)

  val workerName = "332"
  val defaultWorkerRpcPort = 50050

  def main(args: Array[String]): Unit = {
    logger.info("Starting worker ")

    val (masterHost, masterPort, inputDirs, outputDir, bindingPort) = parseArgs(args)
    val thisWorkerRpcPort = if (bindingPort.isDefined) bindingPort.get else defaultWorkerRpcPort

    logger.info("Master host: " + masterHost)
    logger.info("Master port: " + masterPort)
    logger.info("Input directory: " + inputDirs)
    logger.info("Output directory: " + outputDir)
    logger.info("This worker's RPC port: " + thisWorkerRpcPort)

    val master = Master(masterHost, masterPort)

    // Initialize directory structure
    val (partitionDir, receivedDir) = initDirectoryStructure(outputDir)
    val partitionWorker = new Worker(inputDirs, partitionDir)
    val blocks = partitionWorker.blocks

    logger.info("Worker ready")
    master.sendReadySignal(workerName, thisWorkerRpcPort)

    // Sample from blocks and send them to master
    val sample = partitionWorker.sample(blocks)

    logger.info("Sample from blocks : " + sample.length)

    val (bytes, workerIpList) = master.sendKeyRange(workerName, sample.length, sample)

    logger.info("Worker gets key range from master")
    logger.info("Another workers: " + workerIpList)

    // Partition blocks by given key range
    val keyRange = bytes map { byte => Key(byte.toByteArray.toList) }

    logger.info("Partition started")

    val partitionedBlocks = partitionWorker.partition(blocks, keyRange)

    logger.info("Partition completed")

    // Sort partitioned blocks
    for (block <- partitionedBlocks) {
      block.sortThenSave
    }

    logger.info("Each partitioned blocks are now sorted")

    // Start server for receiving blocks
    // print(workerIpList)
    val workerClient = WorkerClient(workerIpList)
    val workerServer = new WorkerServer(receivedDir)
    workerServer.start(thisWorkerRpcPort)

    logger.info("Ready to send partition to other workers")

    // Send signal to master to sync.
    master.partitionComplete(workerName)

    logger.info("Partition send started")

    // Repeatedly send partitioned blocks to other workers
    for {
      block <- partitionedBlocks
      workerIdx <- block.partitionIdx
    } yield {
      val destIP = workerIpList(workerIdx)
      val byteStringList = block.toList map { tuple => tuple.toByteString }
      workerClient.sendPartition(workerName, destIP, byteStringList)
    }

    logger.info("Partition send completed")

    // Send signal to master to sync.
    master.exchangeComplete(workerName)
    workerServer.stop()

    logger.info("Merge started")

    val mergeWorker = new Worker(List(receivedDir), outputDir)
    val recievedBlocks = mergeWorker.blocks
    val mergedBlocks = mergeWorker.merge(recievedBlocks)

    logger.info("Merge completed")

    master.sendFinishSignal(workerName)

    logger.info("Removing temp files...")

    removeFilesInDir(receivedDir)
    removeFilesInDir(partitionDir)

    logger.info("Complete!")
  }

  def parseArgs(args: Array[String]): (String, Int, List[String], String, Option[Int]) = {

    argsLengthCheck(args)

    val (masterHost, masterPort) = getMasterIP(args(0))
    val inputDirs = getInputDirs(args)
    val outputDir = getOutputDir(args)
    val bindingPort = getBindingPort(args)

    (masterHost, masterPort, inputDirs, outputDir, bindingPort)
  }

  def argsLengthCheck(args: Array[String]): Unit = {
    if (args.length <= 2) throw new Exception("Need more arugments to run")
  }

  def getMasterIP(arg: String): (String, Int) = {
    arg.split(":") match {
      case Array(host, port) => (host, port.toInt)
      case _ => throw new Exception("Provide master address")
    }
  }

  def getInputDirs(args: Array[String]): List[String] = {
    val inputDirOpt = args(1)
    require { inputDirOpt == "-I" }

    args.drop(2).takeWhile(arg => arg != "-O").toList
  }

  def getOutputDir(args: Array[String]): String = {
    val idx = args.indexWhere(arg => arg == "-O")
    if (idx == -1) throw new Exception("Provide output directory using -O option")

    val leftArgs = args.drop(idx + 1)
    if (leftArgs.length == 0) throw new Exception("Provide output directory")

    leftArgs(0)
  }

  def getBindingPort(args: Array[String]): Option[Int] = {
    val idx = args.indexWhere(arg => arg == "-P" || arg == "-p")
    if (idx == -1) return None

    val leftArgs = args.drop(idx + 1)
    if (leftArgs.length == 0) throw new Exception("Provide port number to bind to")

    Some(leftArgs(0).toInt)
  }

  def setUpTempDir(dir: String): Unit = {
    val tempDir = new File(dir)
    tempDir.mkdirs()
  }

  def initDirectoryStructure(dir: String): (String, String) = {
    val partitionDir = dir + "/partitioned"
    val receivedDir = dir + "/received"
    setUpTempDir(partitionDir)
    setUpTempDir(receivedDir)
    (partitionDir, receivedDir)
  }

  def removeFilesInDir(dir: String): Unit = {
    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      if (file.exists && !file.delete) {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
    }
    deleteRecursively(new File(dir))
  }
}

class WorkerServer(val saveDir: String) extends DistSortServer(ExecutionContext.global) {

  // Thie variable needs to be modified due to possible race condition.
  private val lock = new ReentrantReadWriteLock()
  private val read = lock.readLock()
  private val write = lock.writeLock()
  private var idx = 0

  override def handlePartition(receivedData: List[ByteString]): Unit = {
    val tuples = receivedData map { byte => Tuple(byte.toByteArray.toList) }
    var curIdx = 0

    try {
      write.lock()
      curIdx = idx
      idx = idx + 1
    } finally write.unlock()

    Block.fromTuples(saveDir + s"/$curIdx", tuples)
  }

}