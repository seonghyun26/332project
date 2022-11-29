package test

import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import test.util._


class GenSortSuite extends AnyFunSuite {

    loadPartition()

    test("Partition Generation Test") {

        val source = Source.fromFile("./temp/partition1", "ISO8859-1")

        val byte_list = source.take(100).toList.map {_.toByte}
        assert(byte_list != List())
    }

}