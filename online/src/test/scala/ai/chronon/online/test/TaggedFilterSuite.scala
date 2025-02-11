package ai.chronon.online.test

import org.scalatest._

/** SuiteMixin that skips execution of the tests in a suite if the tests are not triggered
  * by the specific tagName. As an example:
  * sbt test -> Will skip the test suite
  * sbt spark/test -> Will skip the test suite
  * sbt "spark/testOnly -- -n foo" -> Will include the tests in the suite if tagName = foo
  * This allows us to skip some tests selectively by default while still being able to invoke them individually
  */
trait TaggedFilterSuite extends SuiteMixin { this: Suite =>

  def tagName: String

  // Override to filter tests based on tags
  abstract override def run(testName: Option[String], args: Args): Status = {
    // If the tagName is explicitly included, run normally
    val include = args.filter.tagsToInclude match {
      case Some(tags) => tags.contains(tagName)
      case _          => false
    }

    val emptyFilter = Filter.apply()
    val argsWithTagsCleared = args.copy(filter = emptyFilter)
    if (include) {
      super.run(testName, argsWithTagsCleared)
    } else {
      // Otherwise skip this suite
      SucceededStatus
    }
  }
}
