package ai.chronon.orchestration.temporal.converter

/** This class provides default values for all Java types, as defined by the JLS.
  * For non-primitive types and void, null is returned
  */
object Defaults {

  def defaultValue[T](`type`: Class[T]): T = {
    if (`type`.isPrimitive) {
      if (`type` eq classOf[Boolean]) return false.asInstanceOf[T]
      else if (`type` eq classOf[Char]) return 0.toChar.asInstanceOf[T]
      else if (`type` eq classOf[Byte]) return 0.toByte.asInstanceOf[T]
      else if (`type` eq classOf[Short]) return 0.toShort.asInstanceOf[T]
      else if (`type` eq classOf[Int]) return 0.asInstanceOf[T]
      else if (`type` eq classOf[Long]) return 0L.asInstanceOf[T]
      else if (`type` eq classOf[Float]) return 0.0f.asInstanceOf[T]
      else if (`type` eq classOf[Double]) return 0.0.asInstanceOf[T]
    }
    null.asInstanceOf[T]
  }
}
