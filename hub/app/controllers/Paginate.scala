package controllers

trait Paginate {
  val defaultOffset = 0
  val defaultLimit = 10
  val maxLimit = 100

  def paginateResults[T](results: Seq[T], offset: Int, limit: Int): Seq[T] = {
    results.slice(offset, offset + limit)
  }
}
