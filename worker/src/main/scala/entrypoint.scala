
package worker

import sys.process._

import common._

import network.distsort.client._
import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext

object Entrypoint {

  val workerName = "332"

  def main(args: Array[String]): Unit = {
    val (masterHost, masterPort, inputDirs, outputDir) = parseArgs(args)
    val client = DistSortClient(masterHost, masterPort)

    // Initialize directory structure
    initDirectoryStructure(outputDir)
    val partitionWorker = new Worker(inputDirs, partitionDir)
    val blocks = partitionWorker.blocks

    client.sendReadySignal(workerName, workerName)

    // Sample from blocks and send them to master
    val sample = partitionWorker.sample(blocks)
    val (bytes, workerIpList) = client.sendKeyRange(workerName, sample.length, sample)

    // Partition blocks by given key range
    val keyRange = bytes map { byte => Key(byte.toByteArray.toList) }
    val partitionedBlocks = partitionWorker.partition(blocks, keyRange)

    // Sort partitioned blocks
    for (block <- partitionedBlocks) {
      block.sortThenSave
    }

    // Start server for receiving blocks
    val workerServer = new DistSortWorkerServer(ExecutionContext.global) with PartitionHandler(outputDir + "/recieved")
    workerServer.start()

    // Send signal to master to sync.
    client.partitionComplete(workerName)

    // Repeatedly send partitioned blocks to other workers
    for {
      block <- partitionedBlocks
      workerIdx <- block.partitionIdx
    } yield {
      val destIP = workerIpList(workerIdx)
      val byteStringList = block.toList map { tuple => tuple.toByteString }
      client.sendPartition(workerName, destIP, byteStringList)
    }

    // Send signal to master to sync.
    client.exchangeComplete(workerName)
    workerServer.stop()

    val mergeWorker = new Worker(receivedDir, outputDir)
    val recievedBlocks = List()
    worker.merge(recievedBlocks)
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

  def initDirectoryStructure(dir: String): Unit = {
    val partitionDir = outputDir + "/partitioned"
    val recievedDir = outputDir + "/received"
    setUpTempDir(partitionDir)
    setUpTempDir(recievedDir)
  }
}