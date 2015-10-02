package bbejeck.sorting

import bbejeck.AirlineFlightUtils._
import bbejeck.SparkJob
import bbejeck.Utils._

/**
 * Created by bbejeck on 9/18/15.
 *
 */
object SecondarySort extends SparkJob {

  def runSecondarySortExample(args: Array[String]): Unit = {

    val sc = context("SecondarySorting")
    val rawDataArray = sc.textFile(args(0)).map(line => line.split(","))
    val airlineData = rawDataArray.map(arr => createKeyValueTuple(arr))
    val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(5))

    //only done locally for demo purposes, usually write out to HDFS
    keyedDataSorted.collect().foreach(println)
  }

  def createKeyValueTuple(data: Array[String]) :(FlightKey,List[String]) = {
      (createKey(data),listData(data))
  }

  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(UNIQUE_CARRIER), safeInt(data(DEST_AIRPORT_ID)), safeDouble(data(ARR_DELAY)))
  }

  def listData(data: Array[String]): List[String] = {
    List(data(FL_DATE), data(ORIGIN_AIRPORT_ID), data(ORIGIN_CITY_MARKET_ID), data(DEST_CITY_MARKET_ID))
  }

  /* Sample snippets for future use
      //Example of what to do to strip off first line of file
    val rawData = sc.textFile(args(0)).mapPartitionsWithIndex((i, line) => skipLines(i, line, 1))
    //example of keyBy but retains entire array in value
    val keyedByData = rawDataArray.keyBy(arr => createKey(arr))
     val finalData = keyedDataSorted.map({ case (key, list) => DelayedFlight.fromKeyAndData(key, list) })

    //finalData.collect().foreach(println)

   */


}
