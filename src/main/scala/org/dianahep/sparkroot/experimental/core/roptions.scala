package org.dianahep.sparkroot.experimental.core

class RMap(m: Map[String, String]) {
  private val map = m.map {case (key: String, value: String) => (key.toLowerCase, value)} 

  def get(key: String): Option[String] = map.get(key)
}

case class ROptions(options: RMap) {
  def this(m: Map[String, String]) =
    this(new RMap(m))

  def get(key: String): Option[String] = options.get(key)
}

object ROptions {
  def apply(m: Map[String, String]): ROptions =
    ROptions(new RMap(m))
}
