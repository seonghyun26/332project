package master.util.sync

import scala.concurrent.SyncVar
import java.util.logging.Logger

abstract class SyncAcc[T](initialValue: T) extends SyncVar[T] {
  this.put(initialValue)

  def accumulate(newValue: T): Unit
}

class SyncAccList[T](initialValue: List[T]) extends SyncAcc[List[T]](initialValue) {
  private val logger = Logger.getLogger(classOf[SyncAccList[T]].getName)
  def accumulate(incoming: List[T]): Unit = {
    val taken = this.take
    logger.finer(s"Value taken: $taken")
    logger.finer(s"Accumulating value: $incoming")
    this.put(taken ++ incoming)
    logger.finer("Value put")
  }
}

class SyncAccInt(initialValue: Int) extends SyncAcc[Int](initialValue) {
  private val logger = Logger.getLogger(classOf[SyncAccInt].getName)
  def accumulate(incoming: Int): Unit = {
    val taken = this.take
    logger.finer(s"Value taken: $taken")
    logger.finer(s"Accumulating value: $incoming")
    this.put(taken + incoming)
    logger.finer("Value put")
  }
}
