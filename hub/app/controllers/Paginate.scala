package controllers

import model.Model

trait Paginate {
  val defaultOffset = 0
  val defaultLimit = 10
  val maxLimit = 100

  def paginateResults(results: Seq[Model], offset: Int, limit: Int): Seq[Model] = {
    results.slice(offset, offset + limit)
  }
}
