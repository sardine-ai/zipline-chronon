package ai.chronon.orchestration.utils

import scala.collection.Seq

object TablePrinter {
  def printTable[T](headers: Seq[String], data: Seq[Seq[T]]): Unit = {
    if (headers.isEmpty || data.isEmpty) {
      println("No data to display")
      return
    }

    // Convert all values to strings and find the maximum width for each column
    val stringData = data.map(_.map(_.toString))
    val colWidths = headers.indices.map { i =>
      val colValues = stringData.map(_(i))
      (colValues :+ headers(i)).map(_.length).max
    }

    // Create the format string for each row
    val formatStr = colWidths.map(w => s"%-${w}s").mkString(" | ")

    // Print headers
    println("+" + colWidths.map(w => "-" * (w + 2)).mkString("+") + "+")
    println("| " + String.format(formatStr, headers.map(_.asInstanceOf[AnyRef]).toSeq: _*) + " |")
    println("+" + colWidths.map(w => "-" * (w + 2)).mkString("+") + "+")

    // Print data
    stringData.foreach { row =>
      println("| " + String.format(formatStr, row.map(_.asInstanceOf[AnyRef]).toSeq: _*) + " |")
    }
    println("+" + colWidths.map(w => "-" * (w + 2)).mkString("+") + "+")
  }

  // Convenience method for single-type tables
  def printTypedTable[T](headers: Seq[String], data: Seq[T])(implicit extractor: T => Seq[Any]): Unit = {
    printTable(headers, data.map(extractor))
  }
}

// Example usage:
object TablePrinterExample extends App {
  // Basic usage with raw data
  val headers: Seq[String] = Seq("Name", "Age", "City")
  val data: Seq[Seq[Any]] = Seq(
    Seq("John Doe", 30, "New York"),
    Seq("Jane Smith", 25, "Los Angeles"),
    Seq("Bob Johnson", 35, "Chicago")
  )

  println("Basic Table Example:")
  TablePrinter.printTable(headers, data)

  // Example with case class
  case class Person(name: String, age: Int, city: String)

  val people: Seq[Person] = Seq(
    Person("John Doe", 30, "New York"),
    Person("Jane Smith", 25, "Los Angeles"),
    Person("Bob Johnson", 35, "Chicago")
  )

  println("\nTyped Table Example:")
  implicit val personExtractor: Person => Seq[Any] = p => Seq(p.name, p.age, p.city)
  TablePrinter.printTypedTable(headers, people)
}
