
import java.io._

package object common {
    type ??? = Nothing

    type *** = Any

    def writeBytes(data: Stream[Byte], file: File) = {
        val target = new BufferedOutputStream( new FileOutputStream(file) )
        try data.foreach{ target.write(_) } finally target.close
    }

}
