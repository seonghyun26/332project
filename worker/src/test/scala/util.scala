
package test

import sys.process._
import java.io.File

package object util{
  def loadPartition(): Unit = {
    buildGenSort()

    makeDir("./temp")

    makeBlock("./temp/partition1", 1000)
  }

  def buildGenSort(): Unit = {
    if (!(new File("./gensort")).exists())
      "./scripts/get-gensort.sh -q" !
  }

  def makeDir(path: String): Unit = {
    if (!(new File(path)).exists())
      "mkdir " + path !
  }

  def makeBlock(path: String, size: Int): Unit = {
    s"./gensort/gensort -b $size $path" !
  }
}
