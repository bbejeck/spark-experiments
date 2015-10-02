package bbejeck

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bbejeck on 9/18/15.
 */
trait SparkJob {

  //TODO add implicit functions for converting tuples and other values to objects
  //TODO use function from TupleConversion

  setApacheLoggingToError()

  def context(appName: String): SparkContext = {
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    sc
  }


  def skipLines[T](index: Int, lines: Iterator[T], num: Int): Iterator[T] = {
    if (index == 0) {
      lines.drop(num)
    }
    lines
  }





  def setApacheLoggingToError():Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  }

}
