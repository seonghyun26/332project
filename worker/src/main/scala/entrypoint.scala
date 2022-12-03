
package worker

import sys.process._
import java.io.File

import common._

import network.rpc.master.client.{DistSortClient => Master}

import network.rpc.worker.client.{DistSortClient => WorkerClient}
import network.rpc.worker.server.{DistSortServer}

import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext
import java.util.concurrent.locks.ReentrantReadWriteLock

object Entrypoint {

  val workerName = "332"

  def main(args: Array[String]): Unit = {
    val (masterHost, masterPort, inputDirs, outputDir) = parseArgs(args)
    val master = Master(masterHost, masterPort)

    // Initialize directory structure
    val (partitionDir, receivedDir) = initDirectoryStructure(outputDir)
    val partitionWorker = new Worker(inputDirs, partitionDir)
    val blocks = partitionWorker.blocks

    master.sendReadySignal(workerName, workerName)

    // Sample from blocks and send them to master
    val sample = partitionWorker.sample(blocks)
    val (bytes, workerIpList) = master.sendKeyRange(workerName, sample.length, sample)

    // Partition blocks by given key range
    val keyRange = bytes map { byte => Key(byte.toByteArray.toList) }
    val partitionedBlocks = partitionWorker.partition(blocks, keyRange)

    // Sort partitioned blocks
    for (block <- partitionedBlocks) {
      block.sortThenSave
    }

    // Start server for receiving blocks
    val workerClient = WorkerClient(masterHost, masterPort)
    val workerServer = new WorkerServer(receivedDir)
    workerServer.start()

    // Send signal to master to sync.
    master.partitionComplete(workerName)

    // Repeatedly send partitioned blocks to other workers
    for {
      block <- partitionedBlocks
      workerIdx <- block.partitionIdx
    } yield {
      val destIP = workerIpList(workerIdx)
      val byteStringList = block.toList map { tuple => tuple.toByteString }
      workerClient.sendPartition(workerName, destIP, byteStringList)
    }

    // Send signal to master to sync.
    master.exchangeComplete(workerName)
    workerServer.stop()

    val mergeWorker = new Worker(List(receivedDir), outputDir)
    val recievedBlocks = mergeWorker.blocks
    val mergedBlocks = mergeWorker.merge(recievedBlocks)

    master.sendFinishSignal(workerName)
  }

  def parseArgs(args: Array[String]): (String, Int, List[String], String) = {

    argsLengthCheck(args)

    val (masterHost, masterPort) = getMasterIP(args(0))
    val inputDirs = getInputDirs(args)
    val outputDir = getOutputDir(args)

    (masterHost, masterPort, inputDirs, outputDir)
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
    if (idx == -1) throw new Exception("Provide output directory")

    val leftArgs = args.drop(idx + 1)
    if (leftArgs.length > 1) throw new Exception("Too many Arguments")

    leftArgs(0)
  }

  def setUpTempDir(dir: String): Unit = {
    val tempDir = new File(dir + "/temp")
    tempDir.mkdirs()
  }

  def initDirectoryStructure(dir: String): (String, String) = {
    val partitionDir = dir + "/partitioned"
    val receivedDir = dir + "/received"
    setUpTempDir(partitionDir)
    setUpTempDir(receivedDir)
    (partitionDir, receivedDir)
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