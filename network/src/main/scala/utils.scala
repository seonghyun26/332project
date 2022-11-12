package network.utils

import java.net.{ ServerSocket, Socket }
import java.io.InputStreamReader
import java.io.OutputStreamWriter

class ReadWriteSocket(connection: Socket) {
  val outputStream = new OutputStreamWriter(connection.getOutputStream)
  val inputStream = new InputStreamReader(connection.getInputStream)

  def read_string(size: Int): String = {
    val buffer = new Array[Char](size)
    val _ = this.inputStream.read(buffer)
    new String(buffer)
  }

  def write_string(string: String) = {
    this.outputStream.write(string)
    this.outputStream.flush()
  }
}