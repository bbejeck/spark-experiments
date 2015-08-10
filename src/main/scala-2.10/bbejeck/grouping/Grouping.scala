package bbejeck.grouping

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * Created by bbejeck on 7/19/15.
 */
object Grouping {

  def runGroupingExample(args: Array[String]) = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("Grouping Examples"))

    if (args.isEmpty) {
      println("Must supply name of person file")
      sys.exit(1)
    }
    val peopleList = sc.textFile(args(0))


    val peopleAtAddress = peopleList.map(_.split(",")).map(line => (line(2), line(0) + " " + line(1))).cache()

    val groupByResults = peopleAtAddress.groupByKey()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s:mutable.HashSet[String],v:String) => s += v
    val mergePartitionSets =(p1:mutable.HashSet[String],p2:mutable.HashSet[String]) => p1 ++= p2

    val aggregateResults = peopleAtAddress.aggregateByKey(initialSet)(addToSet,mergePartitionSets)

    val initialCount = 0;
    val addToCounts = (n:Int, v:String) => n + 1
    val sumPartitionCounts = (p1:Int, p2:Int) => p1 + p2

    val aggegregateCounts = peopleAtAddress.aggregateByKey(initialCount)(addToCounts,sumPartitionCounts)

    //Experimental working with changing soure
    //val testAgg = peopleAtAddress.aggregateByKeyToList()


    println("GroupByKey results")
    val displayNumber = 5

    val groupBy = groupByResults.filter(t => t._2.size > 1).collect()
    for (ind <- 0 to displayNumber) {
      printKeysAndValues(groupBy(ind))
    }

    println("------------------------")
    println("AggregateByKey results")
    val aggregateBy = aggregateResults.filter(t => t._2.size > 1).collect()
    for (ind <- 0 to displayNumber) {
      printKeysAndValues(aggregateBy(ind))
    }


//    println("------------------------")
//    println("Test AggregateByKeyToList results")
//    val testAggregateBy = testAgg.filter(t => t._2.size > 1).collect()
//    for (ind <- 0 to displayNumber) {
//      printKeysAndValues(testAggregateBy(ind))
//    }

    println("-------------------------")
    val testCountResults = aggegregateCounts.filter(t => t._2 > 1)collect()
    for (ind <- 0 to displayNumber){
        val t = testCountResults(ind)
        println(t._1 +" -> "+t._2 )
    }
  }

  def printKeysAndValues(x: (String, Iterable[String])): Unit = {
    println(x._1 + " -> " + x._2.mkString(";"))
  }


}
