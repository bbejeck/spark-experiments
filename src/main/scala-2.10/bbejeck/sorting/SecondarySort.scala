package bbejeck.sorting

import bbejeck.AirlineFlightUtils._
import bbejeck.SparkJob
import bbejeck.Utils._
import bbejeck.broadcast.GuavaTableLoader
import com.google.common.collect.Table
import org.apache.spark.broadcast.Broadcast

/**
 * Created by bbejeck on 9/18/15.
 *
 */
object SecondarySort extends SparkJob {

  type RefTable = Table[String, String, String]


  def runSecondarySortExample(args: Array[String]): Unit = {

    val dataPath = args(0)
    val refDataPath = args(1)
    val refDataFiles = args(2)


    val sc = context("SecondarySorting")
    val rawDataArray = sc.textFile(dataPath).map(line => line.split(","))

    val table = GuavaTableLoader.load(refDataPath, refDataFiles)

    val bcTable = sc.broadcast(table)

    val airlineData = rawDataArray.map(arr => createKeyValueTuple(arr))
    val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(5))

    val translatedData = keyedDataSorted.map(t => createDelayedFlight(t._1, t._2, bcTable))

    //printing out only done locally for demo purposes, usually write out to HDFS
    translatedData.collect().foreach(println)
  }

  def createDelayedFlight(key: FlightKey, data: List[String], bcTable: Broadcast[RefTable]): DelayedFlight = {
    val table = bcTable.value
    val airline = table.get(AIRLINE_DATA, key.airLineId)
    val destAirport = table.get(AIRPORT_DATA, key.arrivalAirportId.toString)
    val destCity = table.get(CITY_DATA, data(3))
    val origAirport = table.get(AIRPORT_DATA, data(1))
    val originCity = table.get(CITY_DATA, data(2))

    DelayedFlight(airline, data.head, origAirport, originCity, destAirport, destCity, key.arrivalDelay)
  }

  def createKeyValueTuple(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), listData(data))
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
