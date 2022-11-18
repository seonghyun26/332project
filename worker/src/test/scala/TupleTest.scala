package test

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import worker.Tuple

@RunWith(classOf[JUnitRunner])
class ListsSuite extends FunSuite {

    val source = Source.fromFile("./worker/partition1", "ISO8859-1")

    val byte_list = source.take(100).toList.map {_.toByte}

    test("Tuple Test") {
        assert(Tuple(byte_list).key == byte_list.take(10))
        assert(Tuple(byte_list).value == byte_list.dropRight(90))
    }

}