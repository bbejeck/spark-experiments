package bbejeck.cornercases


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bbejeck on 8/14/15.
 */
object StripFirstLines {

  def stripLinesExample(args: Array[String]) {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("Stripping Lines"))

    val numLinesToSkip = if (args.length == 0) 1 else args(0).toInt

    val inputData = sc.parallelize(Array("Skip this line XXXXX","Start here instead AAAA","Second line to work with BBB"))

    val valuesRDD = inputData.mapPartitionsWithIndex((idx, iter) => skipLines(idx, iter, numLinesToSkip))

    val linesWithoutFirst = valuesRDD.collect()

    for (x <- linesWithoutFirst ) {
        println(x)
    }


  }

  def skipLines(index: Int, lines: Iterator[String], num: Int): Iterator[String] = {
    if (index == 0) {
      lines.drop(num)
    }
    lines
  }


}
