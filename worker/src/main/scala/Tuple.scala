package worker

import java.lang.IllegalArgumentException
import scala.math.BigInt

import com.google.protobuf.ByteString

import common._


object Tuple{
    // Tuple(List[Byte]) would be a constructor of tuple.
  def apply(byte: List[Byte]): Tuple = Tuple.fromBytes(byte)

  def fromBytes(byte: List[Byte]): Tuple = {
    require(byte.length == 100)
    val (key, value) = byte.splitAt(10)
    new Tuple(key, value)
  }

  implicit class Tuples(tuples: List[Tuple]) {
    def toBytes: List[Byte] = {
      val byteList = tuples.foldRight(List[Byte]()){
        (tuple: Tuple, acc: List[Byte]) => tuple.toBytes ::: acc
      }

      // Check if order preserved.
      assert {byteList.take(10) == tuples(0).key.value}

      byteList
    }
  }
}

class Tuple(key_ :List[Byte], value_ :List[Byte]) extends Comparable[Tuple] {
  require(key_.length == 10)
  require(value_.length == 90)

  val key: Key = Key(key_)
  val value: List[Byte] = value_

  def byteToString(byte: Byte): String = {
    val str = byte.toHexString.takeRight(2)
    ("00" + str).substring(str.length)
  }

  def byteListToString(byte_list: List[Byte]): String = {
    (byte_list map byteToString).mkString(" ")
  }

  override def toString(): String = {
    "KEY | %s".format(byteListToString(key.value))
  }

  def toByteString: ByteString = {
    ByteString.copyFrom(toBytes.toArray)
  }

  override def <(other: Tuple): Boolean = key < other.key

  override def ==(other: Tuple): Boolean = key == other.key

  def toBytes: List[Byte] = (key.value ++ value)
}