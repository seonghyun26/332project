
package worker

trait Comparable[T <: Comparable[T]] {
  def <(other: T): Boolean
}

trait Sortable[T <: Comparable[T]] {
  def sortCheck(list: List[T]): Boolean = {
    if (list.isEmpty || list.tail.isEmpty) {
      true
    }
    else if (list.head < list.tail.head) {
      sortCheck(list.tail)
    }
    else {
      false
    }
  }

  def sort(list: List[T]): List[T] = {
    list.sortWith((a,b)=> a<b)
  }
}
