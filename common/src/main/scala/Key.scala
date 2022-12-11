package common

import com.google.protobuf.ByteString

object Key{
  final val MIN = Key.fromBytes(List.fill(10)(0.toByte))
  final val MAX = Key.fromBytes(List.fill(10)(255.toByte))

  def apply(str: String): Key = new Key(str)

  def fromBytes(bytes: Iterable[Byte]): Key = new Key(bytes.map(_.toChar).mkString)
}

class Key(val value: String) extends Comparable[Key]{
  def length = value.length

  def asBytes = value.map(_.toByte).toArray

  def toByteString = ByteString.copyFrom(asBytes)

  override def <(other: Key): Boolean = {
    require(length == other.length)
    value < other.value
  }

  override def ==(other: Key): Boolean = {
    value == other.value
  }

}