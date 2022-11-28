
package test

import sys.process._
import java.io.File

object Partition{
  def loadPartition(): Unit = {
    if (!(new File("./gensort")).exists())
      "./scripts/get-gensort.sh -q" !

    if (!(new File("./temp")).exists())
      "mkdir ./temp" !

    "./gensort/gensort -b 1000 ./temp/partition1" !

  }
}
