package ai.chronon.orchestration.utils

import ai.chronon.api.Constants

import java.security.MessageDigest

object StringExtensions {

  lazy val digester: ThreadLocal[MessageDigest] = new ThreadLocal[MessageDigest]() {
    override def initialValue(): MessageDigest = MessageDigest.getInstance("MD5")
  }

  implicit class StringOps(s: String) {
    def md5: String =
      digester
        .get()
        .digest(s.getBytes(Constants.UTF8))
        .map("%02x".format(_))
        .mkString
        .take(8)
  }
}
