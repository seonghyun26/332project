import org.scalatest.funsuite.AnyFunSuite
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import common.FutureExt.FutureCompanionOps
import master.Entrypoint.{main => masterMain}
import it.rpc.master.client.DummyWorker

class MasterRpcTest extends AnyFunSuite {
  implicit private val ec = ExecutionContext.global

  test("dummy integration test") {
    println("dummy integration test success")
  }

  test("single master single worker") {
    val masterIpAddress = "127.0.0.1"
    val masterPort = 10000
    val workerPort = 20000
    val numWorkers = 1
    val masterTask = Future {
      masterMain(Array(numWorkers.toString, masterPort.toString))
    }
    val workerTask = Future {
      val worker = new DummyWorker("worker-0", masterIpAddress, masterPort)
      worker.run(workerPort)
    }
    val tasks = List(masterTask, workerTask)
    Await.result(Future.all(tasks), 10 seconds)
  }

  test("single master multiple worker - 1") {
    val masterIpAddress = "127.0.0.1"
    val masterPort = 10100
    val workerPortBase = 20100
    val numWorkers = 3
    val masterTask = Future {
      masterMain(Array(numWorkers.toString, masterPort.toString))
    }
    val workerTasks = for (i <- (0 until numWorkers).toList) yield Future {
      val worker = new DummyWorker(s"worker-$i", masterIpAddress, masterPort)
      worker.run(workerPortBase + i)
    }
    val tasks = List(masterTask) ++ workerTasks
    Await.result(Future.all(tasks), 10 seconds)
  }
}
