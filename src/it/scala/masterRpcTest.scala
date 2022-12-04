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
    val masterPort = 34632
    val workerPort = 32871
    val numWorkers = 1
    val masterTask = Future {
      masterMain(Array(numWorkers.toString, masterPort.toString))
    }
    val workerTask = Future {
      val worker = new DummyWorker("worker-1", workerPort, "127.0.0.1", masterPort)
      worker.run()
    }
    val tasks = List(masterTask, workerTask)
    Await.result(Future.all(tasks), 10 seconds)
  }
}
