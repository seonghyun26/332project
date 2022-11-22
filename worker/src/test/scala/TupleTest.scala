package test

import java.lang.IllegalArgumentException
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import worker.Tuple

import sys.process._


class TupleSuite extends AnyFunSuite {

    "./scripts/get-gensort.sh -q" !

    "mkdir temp" !

    "./gensort/gensort -b 1000 ./temp/partition1" !

    test("Tuple Load Test") {

        val source = Source.fromFile("./temp/partition1", "ISO8859-1")

        val byte_list = source.take(100).toList.map {_.toByte}

        assert(Tuple(byte_list).key == byte_list.take(10))
        assert(Tuple(byte_list).value == byte_list.drop(10))
    }

    test("Tuple Spec Test 1") {
        val byte_list: List[Byte] = (1 to 110).toList.map {_.toByte}

        assertThrows[IllegalArgumentException]{
            Tuple(byte_list)
        }

    }

    test("Tuple Comparison Test 1") {

        val byte_list1: List[Byte] = (1 to 100).toList.map {_.toByte}
        val byte_list2: List[Byte] = (2 to 101).toList.map {_.toByte} 

        assert(Tuple(byte_list1) < Tuple(byte_list2))

    }

    test("Tuple Comparison Test 2") {

        val byte_list1: List[Byte] = (1 to 100).toList.map {_.toByte}

        assert(!(Tuple(byte_list1) < Tuple(byte_list1)))

    }
}