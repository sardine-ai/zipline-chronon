package ai.chronon.online.fetcher

import ai.chronon.online.KVStore

import scala.concurrent.{ExecutionContext, Future}

/** Scala context switches between chains of futures. This fuses all operations into the same thread
  * by fusing the passed in functions.
  */
class ChainedFuture[Input, Current](fut: Future[Input], val func: Input => Current)(implicit ec: ExecutionContext) {

  def chain[Next](nextFunc: Current => Next): ChainedFuture[Input, Next] = {
    new ChainedFuture[Input, Next](fut, func andThen nextFunc)
  }

  def build(): Future[Current] = {
    fut.map(func)
  }

}

object ChainedFuture {
  def apply[T](fut: Future[T])(implicit ec: ExecutionContext): ChainedFuture[T, T] = {
    new ChainedFuture(fut, identity)
  }

  type KvResponseToFetcherResponse = ChainedFuture[Seq[KVStore.GetResponse], Seq[Fetcher.Response]]

}
