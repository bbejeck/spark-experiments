package bbejeck

/**
 * Created by bbejeck on 10/2/15.
 */
object Utils {

  def safeInt(s: String): Int = {
    if (s == null || s.isEmpty) 0 else s.toInt
  }

  def safeDouble(s: String): Double = {
    if (s == null || s.isEmpty) 0.0 else s.toDouble
  }

}
