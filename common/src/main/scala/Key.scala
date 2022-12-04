package common

object Key{
  final val MIN = Key(List.fill(10)(0.toByte))
  final val MAX = Key(List.fill(10)(255.toByte))

  def apply(byte: List[Byte]): Key = new Key(byte)
}

class Key(val value: List[Byte]) extends Comparable[Key]{
  def length = value.length

  def asBytes = value.toArray

  override def <(other: Key): Boolean = {
    require(length == other.length)

    def toUnsigned(a: Byte): Int = {
      if(a < 0) a.toInt + 0x100
      else a.toInt
    }

    def less(a: List[Int], b: List[Int]): Boolean = {
      if (a.isEmpty && b.isEmpty) return false
      else (a.head < b.head) || (a.head == b.head && less(a.tail, b.tail))
    }

    less(value map toUnsigned, other.value map toUnsigned)
  }

  override def ==(other: Key): Boolean = {
    value == other.value
  }

}