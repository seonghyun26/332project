package test

import java.lang.IllegalArgumentException
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import worker.Tuple
import test.util._


class TupleSuite extends AnyFunSuite {

    loadPartition()

    test("Tuple Load Test") {

        val source = Source.fromFile("./temp/partition1", "ISO8859-1")

        val byteList = source.take(100).toList.map {_.toByte}

        assert(Tuple(byteList).key.value == byteList.take(10))
        assert(Tuple(byteList).value == byteList.drop(10))
    }

    test("Tuple Spec Test 1") {
        val byte_list: List[Byte] = (1 to 110).toList.map {_.toByte}

        assertThrows[IllegalArgumentException]{
            Tuple(byte_list)
        }

    }

    test("Tuple Comparison Test 1") {

        val byteList1: List[Byte] = (1 to 100).toList.map {_.toByte}
        val byteList2: List[Byte] = (2 to 101).toList.map {_.toByte} 

        assert(Tuple(byteList1) < Tuple(byteList2))

    }

    test("Tuple Comparison Test 2") {

        val byteList1: List[Byte] = (1 to 100).toList.map {_.toByte}
        assert(!(Tuple(byteList1) < Tuple(byteList1)))

    }
}