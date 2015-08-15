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

    val sc = new SparkContext(new SparkConf().setAppName("Mapping Values"))

    val data = sc.parallelize(Array( (1,"foo"),(2,"bar"),(3,"baz"))).cache()

    val originalPartitions = data.partitions

    val mappedRdd = data.map(x => (x._1,x._2.toUpperCase))

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
