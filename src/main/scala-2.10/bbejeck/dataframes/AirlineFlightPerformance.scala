package bbejeck.dataframes

import bbejeck.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Created by bbejeck on 9/4/15.
 * Demo of DataFrames by Analyzing US Airline Flight Performance
 *
 * From U.S DOT website  https://catalog.data.gov/dataset/airline-on-time-performance-and-causes-of-flight-delays
 * AWS example https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/
 */
object AirlineFlightPerformance extends SparkJob {

  def runInitialFlightPerformanceDataFrame(path: String): Unit = {

    val sc: SparkContext = context("DataFrame Analysis")
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.load(path)

    dataFrame.registerTempTable("airlinePerformance")


    // actualelapsedtime, deptime, arrtime,

    //dataFrame.groupBy("airlineid")

    //        val total = sqlContext.sql("select count(*) from airlinePerformance")
    //        val nullValues = sqlContext.sql("select count(*) from airlinePerformance where depdelayminutes is null")
    //        total.collect().foreach(println)
    //        nullValues.collect().foreach(println)


    val efficientAirports = sqlContext.sql("Select airlineid, dayofmonth, dayofweek," +
      " originairportid, origincitymarketid, destairportid, destcitymarketid, distance, " +
      " AVG(depdelayminutes) as avDepDelay, AVG(arrdelayminutes) as avArrDelay from airlinePerformance  " +
      " where ((depdelayminutes is not null) and (arrdelayminutes is not null) ) " +
      "   limit 25")
    efficientAirports.collect().foreach(println)
    //dataFrame.printSchema()
    //order by airlineid, avDepDelay, avArrDelay desc
  }


}
