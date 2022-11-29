
package common

object Key{
  def apply(byte: List[Byte]): Key = new Key(byte)
}

class Key(val value: List[Byte]) extends Comparable[Key]{
  def length = value.length

  override def <(other: Key): Boolean = {
    require(length == other.length)

    def toUnsigned(a: Byte): Int = {
      if(a < 0) a + 0x100
      else a
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