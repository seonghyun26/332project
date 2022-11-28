
package common

object Key extends Sortable[Key]{
  def apply(byte: List[Byte]): Key = new Key(byte)

  def getRangeIdx(keyRange: List[Key])(value: Key): Int = {
    val idx = (keyRange.indexWhere{p => value < p}) match {
      case -1 => keyRange.length
      case idx => idx
    }

    assert { idx >= 0 && idx <= keyRange.length }
    assert { 
          !(idx != 0 && idx != keyRange.length) ||
          (keyRange(idx-1) <= value && value < keyRange(idx)) 
          }
    idx
  }

}

class Key(val value: List[Byte]) extends Comparable[Key]{
  def length = value.length

  override def <(other: Key): Boolean = {
    require(length == other.length)

    def less(a: List[Byte], b: List[Byte]): Boolean = {
      if (a.isEmpty && b.isEmpty) return false
      else (a.head < b.head) || (a.head == b.head && less(a.tail, b.tail))
      }
    less(value, other.value)
  }

  override def ==(other: Key): Boolean = {
    value == other.value
  }

}