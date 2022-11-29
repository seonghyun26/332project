
package common

import java.lang.IllegalArgumentException


trait Comparable[T <: Comparable[T]] {
  def <(other: T): Boolean

  def ==(other: T): Boolean

  def <=(other: T): Boolean = {
    <(other) || ==(other)
  }

  def >(other: T): Boolean = {
    !(<(other)) && !(==(other))
  }

  def >=(other: T): Boolean = {
    !(<(other))
  }

  /* Check if value is in range [start, end) */
  def inRange(start: Option[T], end: Option[T]): Boolean = {
    (start, end) match {
      case (Some(start), Some(end)) =>(this >= start) && (this < end)
      case (Some(start), None) => (this >= start)
      case (None, Some(end)) => (this < end)
      case (None, None) => throw new IllegalArgumentException("At least one end must have value")
    }
  }
}

trait Sortable[T <: Comparable[T]] {
  def sortCheck(list: List[T]): Boolean = {
    if (list.isEmpty || list.tail.isEmpty) {
      true
    }
    else if (list.head <= list.tail.head) {
      sortCheck(list.tail)
    }
    else {
      false
    }
  }

  def sort(list: List[T]): List[T] = {
    list.sortWith((a,b)=> a<b)
  }

  def mergeStream(streamList: List[Stream[T]]): Stream[T] = {

    val frontList: List[T] = streamList map { stream => stream.head }
    val minIdx = findMinIdx(frontList)

    val newStreamList = streamList.zipWithIndex map {
      case (stream, idx) => if (idx == minIdx) stream.tail else stream
    }

    frontList(minIdx) #:: mergeStream(newStreamList)
  }

  def findMinIdx(list: List[T]): Int = {
    list.indexOf(sort(list)(0))
  }
}
