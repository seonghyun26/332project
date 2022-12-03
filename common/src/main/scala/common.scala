import java.io._
import java.net._


package object common {
    type ??? = Nothing

    type *** = Any

    def writeBytes(data: Iterable[Byte], file: File) = {
        val target = new BufferedOutputStream( new FileOutputStream(file) )
        try data.foreach{ target.write(_) } finally target.close
    }

    def getListOfFiles(dir: String): List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            List[File]()
        }
    }

}
