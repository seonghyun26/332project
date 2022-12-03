
package worker

import common._

import network.distsort.client.DistSortClient
import com.google.protobuf.ByteString


object Entrypoint {

  def main(args: Array[String]): Unit = {
    val (masterHost, masterPort, inputDirs, outputDir) = parseArgs(args)
    val worker = new Worker(inputDirs, outputDir)
    val workerName = "332"
    val client = DistSortClient(masterHost, masterPort)

    val dataBlocks = worker.initialize

    client.sendReadySignal(workerName)
    
    val sample = worker.sample(dataBlocks)

    val (bytes, workers) = client.sendKeyRange(workerName, sample.length, sample)

    val keyRange = bytes map { byte => Key(byte.toByteArray.toList) }

    val partitionedBlocks = worker.partition(dataBlocks, keyRange)

    // TODO: Send Partition to other workers

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
}