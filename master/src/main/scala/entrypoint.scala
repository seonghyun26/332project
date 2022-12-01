import master.server.DistSortServerImpl

object Entrypoint {
  def main(args: Array[String]) = {
    val numWorkers = args.lift(0) match {
      case Some(n) => n.toInt
      case None => throw new Exception("Provide # of workers")
    }
    require(numWorkers > 0)
    // This function blocks the main thread until the server is shut down.
    DistSortServerImpl.serveRPC(numWorkers)
  }
}