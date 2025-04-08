package ai.chronon.online.fetcher

import ai.chronon.online.KVStore

import scala.concurrent.{ExecutionContext, Future}

/** Scala context switches between chains of futures. This fuses all operations into the same thread
  * by fusing the passed in functions.
  */
class ChainedFuture[Input, Output](val fut: Future[Input], val func: Input => Output)(implicit ec: ExecutionContext) {

  def chain[Next](nextFunc: Output => Next): ChainedFuture[Input, Next] = {
    new ChainedFuture[Input, Next](fut, func andThen nextFunc)
  }

  def build(): Future[Output] = {
    fut.map(func)
  }

  def zipMap[OtherInput, OtherOutput, CombinedOutput](
      other: ChainedFuture[OtherInput, OtherOutput],
      coTransform: PartialFunction[(Output, OtherOutput), CombinedOutput])
      : ChainedFuture[(Input, OtherInput), CombinedOutput] = {

    val newFunc: PartialFunction[(Input, OtherInput), CombinedOutput] = { case (first, second) =>
      coTransform(func(first), other.func(second))
    }

    new ChainedFuture[(Input, OtherInput), CombinedOutput](fut zip other.fut, newFunc)
  }
}

object ChainedFuture {
  def apply[T](fut: Future[T])(implicit ec: ExecutionContext): ChainedFuture[T, T] = {
    new ChainedFuture(fut, identity)
  }

  type KvResponseToFetcherResponse = ChainedFuture[Seq[KVStore.GetResponse], Seq[Fetcher.Response]]

}
