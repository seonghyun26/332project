package master.util.sync

import scala.concurrent.SyncVar

abstract class SyncAcc[T](initialValue: T) extends SyncVar[T] {
  this.put(initialValue)

  def accumulate(newValue: T): Unit
}

class SyncAccList[T](initialValue: List[T]) extends SyncAcc[List[T]](initialValue) {
  def accumulate(incoming: List[T]): Unit = {
    this.put(this.take ++ incoming)
  }
}

class SyncAccInt(initialValue: Int) extends SyncAcc[Int](initialValue) {
  def accumulate(incoming: Int): Unit = {
    this.put(this.take + incoming)
  }
}
