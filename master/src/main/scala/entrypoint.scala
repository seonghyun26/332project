import java.net.ServerSocket
import network.utils.ReadWriteSocket

object Entrypoint {
  def handle_request(rwSocket: ReadWriteSocket) = {
    println(rwSocket.read_string(100))
    val message = "message from master"
    rwSocket.write_string(message)
  }

  def create_http_response(content: String) = 
    s"HTTP/1.1 200 OK\r\nContent-Type: plaintext\r\nContent-Length: ${content.length}\r\n\r\n$content"

  def main(args: Array[String]) = {
    val socket = new ServerSocket(44123)
    val client_connection = socket.accept()
    val rwSocket = new ReadWriteSocket(client_connection)
    handle_request(rwSocket)
    client_connection.close()
    socket.close()
  }
}