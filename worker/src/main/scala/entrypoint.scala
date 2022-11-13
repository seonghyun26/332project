import java.net.Socket
import network.utils.ReadWriteSocket
import java.net.Inet4Address
import java.net.InetSocketAddress

object Entrypoint {
  def send_request(rwSocket: ReadWriteSocket) = {
    val message = "message from worker"
    rwSocket.write_string(message)
    val message_from_master = rwSocket.read_string(100).trim
    require(message_from_master == "message from master")
  }

  def main(args: Array[String]) = {
    val (master_host, master_port) = args(0).split(":") match {
      case Array(host, port) => (host, port.toInt)
      case _ => throw new Exception("Provide master address")
    }
    val socket = new Socket(master_host, master_port)
    val rwSocket = new ReadWriteSocket(socket)
    send_request(rwSocket)
    socket.close()
  }
}