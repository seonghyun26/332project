package worker

import java.lang.IllegalArgumentException
import scala.math.BigInt

import com.google.protobuf.ByteString

import common._


object Tuple{
    // Tuple(List[Byte]) would be a constructor of tuple.
  def apply(str: String): Tuple = {
    val (key, value) = str splitAt 10
    new Tuple(key, value)
  }

  def fromBytes(byte: Iterable[Byte]): Tuple = {
    val (key, value) = byte.splitAt(10)
    new Tuple(key.map(_.toChar).mkString, value.map(_.toChar).mkString)
  }

  implicit class Tuples(tuples: List[Tuple]) {
    def toBytes: List[Byte] = {
      def byteList = tuples.foldRight(List[Byte]()){
        (tuple: Tuple, acc: List[Byte]) => tuple.toBytes ::: acc
      }

      // Check if order preserved.
      assert {byteList.take(10) == tuples(0).key.value.map(_.toByte)}

      byteList
    }
  }
}

class Tuple(key_ :String, value_ :String) extends Comparable[Tuple] {
  require(key_.length == 10)
  require(value_.length == 90)

  val key: Key = Key(key_)
  val value: String = value_

  def byteToString(byte: Byte): String = {
    val str = byte.toHexString.takeRight(2)
    ("00" + str).substring(str.length)
  }

  def byteArrayToString(bytes: Array[Byte]): String = {
    (bytes map byteToString).mkString(" ")
  }

  override def toString(): String = {
    "KEY | %s".format(byteArrayToString(key.asBytes))
  }

  def toByteString: ByteString = {
    ByteString.copyFrom(toBytes.toArray)
  }

  override def <(other: Tuple): Boolean = key < other.key

  override def ==(other: Tuple): Boolean = key == other.key

  def toBytes: List[Byte] = (key.value ++ value).map(_.toByte).toList
}