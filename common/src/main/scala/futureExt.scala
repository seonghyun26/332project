package common

import scala.concurrent.{Future, Promise, ExecutionContext}

object FutureExt {
  implicit private val ec = ExecutionContext.global

  implicit class FutureCompanionOps(val f: Future.type) extends AnyVal {

    def always[T](value: T): Future[T] = Promise[T].success(value).future

    def never[T]: Future[T] = Promise[T].future

    def all[T](fs: List[Future[T]]): Future[List[T]] =
      fs match {
        case Nil => Future { Nil }
        case head::tail => head.zip(all(tail)).flatMap { case (head, tail) => Future { head::tail } }
      }
  }
}
