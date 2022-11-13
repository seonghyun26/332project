import java.net.Socket
import network.utils.ReadWriteSocket

object Entrypoint {
  def send_request(rwSocket: ReadWriteSocket) = {
    val message = "message from worker"
    rwSocket.write_string(message)
    println(rwSocket.read_string(100))
  }

  def main(args: Array[String]) = {
    val socket = new Socket("localhost", 44123)
    val rwSocket = new ReadWriteSocket(socket)
    send_request(rwSocket)
    socket.close()
  }
}