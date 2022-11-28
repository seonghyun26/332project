
package worker

object Key extends Sortable[Key]{
  def apply(byte: List[Byte]): Key = new Key(byte)
}

class Key(val value: List[Byte]) extends Comparable[Key]{
  def length = value.length

  def <(other: Key): Boolean = {
    require(length == other.length)

    def less(a: List[Byte], b: List[Byte]): Boolean = {
      if (a.isEmpty && b.isEmpty) return false
      else (a.head < b.head) || (a.head == b.head && less(a.tail, b.tail))
      }
    less(value, other.value)
  }
}