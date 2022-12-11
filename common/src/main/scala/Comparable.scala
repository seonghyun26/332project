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

object Comparable {

  val debug = false

  implicit class ListT[T <: Comparable[T]](list: List[T]){
    def isSorted: Boolean = {
      if (list.isEmpty || list.tail.isEmpty) {
        true
      }
      else if (list.head <= list.tail.head) {
        list.tail.isSorted
      }
      else {
        false
      }
    }

    def sort: List[T] = {
      list.sortWith((a,b)=> a<b)
    }

    def minIdx: Int = {
      require { !list.isEmpty }

      var idx = 0
      var min = list(idx)
      for { (elem, i) <- list.zipWithIndex } yield {
        if (elem < min) {
          idx = i
          min = elem
        } 
      }

      idx
    }

    def getRangeIdx(value: T): Int = {
      val idx = (list.indexWhere{p => value < p}) match {
        case -1 => list.length
        case idx => idx
      }

      assert { idx >= 0 && idx <= list.length }
      assert { 
            !(idx != 0 && idx != list.length) ||
            (list(idx-1) <= value && value < list(idx)) 
            }
      idx
    }
  }

  implicit class ListStreamT[T<: Comparable[T]](streamList: List[Stream[T]]) {
    def mergedStream: Stream[T] = {
      if(streamList.isEmpty) Stream.empty
      else {

        assert {
          !debug || (streamList forall {stream => (!stream.isEmpty)}) 
        }

        def frontList: List[T] = streamList map { stream => stream.head }
        def minIdx = frontList.minIdx

        assert {
          !debug || (frontList.forall( value => frontList(minIdx) <= value ))
        }

        def newStreamList: List[Stream[T]] = 
          for {
            (stream, idx) <- streamList.zipWithIndex
            if (idx != minIdx || !stream.tail.isEmpty)
          } yield {
            if (idx == minIdx) stream.tail
            else stream
          }

        frontList(minIdx) #:: newStreamList.mergedStream
      }
    }
  }
}