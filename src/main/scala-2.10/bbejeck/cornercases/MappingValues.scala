package bbejeck.cornercases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bbejeck on 8/14/15.
 */
object MappingValues {


  def runMappingValues(): Unit = {

    Logger.getLogger ("org.apache").setLevel (Level.ERROR)
    Logger.getLogger ("org.eclipse.jetty.server").setLevel (Level.OFF)

    case class IdText(numberId:Int, text:String)
    implicit def convertTuple = (t:(Int,String)) =>  new IdText(t._1,t._2)



    val sc = new SparkContext(new SparkConf().setAppName("Mapping Values"))

    val data = sc.parallelize(Array( (1,"foo"),(2,"bar"),(3,"baz"))).cache()

    val originalPartitions = data.partitions


    def convertCase(score: IdText) = {
      (score.numberId, score.text.toUpperCase)
    }

    val mappedRdd = data.map(x => convertCase(x))

    val mappedValuesRdd = data.mapValues(s => s.toUpperCase)

    val mappedPartitions = mappedRdd.partitions
    val mappedValuesPartitions = mappedValuesRdd.partitions

    println("Mapping Results")
    printResults(mappedRdd.collect())
    printResults(mappedValuesRdd.collect())

//    Since this is local and tiny amout of data, partitioning remains the same
//    println("Partition Results")
//    println("Original Partitioning "+ originalPartitions)
//    println("Mapping Partitioning "+ mappedPartitions)
//    println("Mapping Values Partitioning "+ mappedValuesPartitions)



    def printResults(res: Array[(Int,String)]): Unit = {
        println(res.mkString(" -> "))
    }


  }

}
