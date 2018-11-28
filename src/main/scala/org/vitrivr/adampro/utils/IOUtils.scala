package org.vitrivr.adampro.utils

import scala.util.Try

trait IOUtils {

  def autoClose[A <: AutoCloseable, B](resource: A)(fun: A ⇒ B): Try[B] = {
    val tryResult = Try {fun(resource)}
    if (resource != null) {resource.close()}
    tryResult
  }
}
