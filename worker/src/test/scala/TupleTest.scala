package test

import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import worker.Tuple

class TupleSuite extends AnyFunSuite {

    test("Tuple Test") {
        val source = Source.fromFile("./worker/partition1", "ISO8859-1")

        val byte_list = source.take(100).toList.map {_.toByte}

        assert(Tuple(byte_list).key == byte_list.take(10))
        assert(Tuple(byte_list).value == byte_list.drop(10))
    }
}