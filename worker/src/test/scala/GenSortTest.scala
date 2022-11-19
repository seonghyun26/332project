package test

import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source


import sys.process._
import java.io.FileNotFoundException


class GenSortSuite extends AnyFunSuite {

    "./scripts/get-gensort.sh -q" !

    "./gensort/gensort -b 1000 ./temp/partition1" !

    test("Partition Generation Test") {

        val source = Source.fromFile("./temp/partition1", "ISO8859-1")

        val byte_list = source.take(100).toList.map {_.toByte}
        assert(byte_list != List())
    }

}