package bbejeck.cornercases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bbejeck on 8/14/15.
 */
object Splitting {

  def runSplitExample() :Unit = {

    Logger.getLogger ("org.apache").setLevel (Level.ERROR)
    Logger.getLogger ("org.eclipse.jetty.server").setLevel (Level.OFF)
    val sc = new SparkContext (new SparkConf ().setAppName ("Splitting Examples") )

    val inputData = sc.parallelize (Array ("foo,bar,baz", "larry,moe,curly", "one,two,three") ).cache ()

    val mapped = inputData.map (line => line.split (",") )
    val flatMapped = inputData.flatMap (line => line.split (",") )

    val mappedResults = mapped.collect ()
    val flatMappedResults = flatMapped.collect ();

    println ("Mapped results of split")
    println (mappedResults.mkString (" : ") )

    println ("FlatMapped results of split")
    println (flatMappedResults.mkString (" : ") )
  }

}
