import java.net.ServerSocket
import network.utils.ReadWriteSocket

object Entrypoint {
  def handle_request(rwSocket: ReadWriteSocket) = {
    val message_from_worker = rwSocket.read_string(100).trim
    require(message_from_worker == "message from worker")
    val message = "message from master"
    rwSocket.write_string(message)
  }

  def main(args: Array[String]) = {
    val num_workers = args.lift(0) match {
      case Some(n) => n.toInt
      case None => throw new Exception("Provide # of workers")
    }
    val port = args.lift(1).getOrElse("44123").toInt
    val socket = new ServerSocket(port)
    val listen_address = s"${socket.getInetAddress.getHostAddress}:${socket.getLocalPort}"
    println(listen_address)

    val addresses_of_workers = for {i <- 0 until num_workers} yield {
      val connection_to_client = socket.accept()
      val client_address = s"${connection_to_client.getRemoteSocketAddress}".stripPrefix("/")
      val rwSocket = new ReadWriteSocket(connection_to_client)
      handle_request(rwSocket)
      connection_to_client.close()
      client_address
    }
    println(addresses_of_workers.mkString(", "))
    socket.close()
  }
}