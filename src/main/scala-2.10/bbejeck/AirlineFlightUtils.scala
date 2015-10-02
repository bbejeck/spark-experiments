package bbejeck

import org.apache.spark.Partitioner
import bbejeck.Utils._

/**
 * Created by bbejeck on 9/18/15.
 */
object AirlineFlightUtils {



  //FlightData columns

  val DAY_OF_MONTH = 0
  val DAY_OF_WEEK = 1
  val FL_DATE = 2
  val UNIQUE_CARRIER = 3
  val CARRIER = 4
  val ORIGIN_AIRPORT_ID = 5
  val ORIGIN_CITY_MARKET_ID = 6
  val ORIGIN_STATE_ABR = 7
  val DEST_AIRPORT_ID = 8
  val DEST_CITY_MARKET_ID = 9
  val DEST_STATE_ABR = 10
  val CRS_DEP_TIME = 11
  val DEP_TIME = 12
  val DEP_DELAY_NEW = 13
  val TAXI_OUT = 14
  val WHEELS_OFF = 15
  val WHEELS_ON = 16
  val TAXI_IN = 17
  val CRS_ARR_TIME = 18
  val ARR_TIME = 19
  val ARR_DELAY = 20
  val CANCELLED = 21
  val CANCELLATION_CODE = 22
  val DIVERTED = 23

  case class DelayedFlight(airLineId: String,
                           date: String,
                           originAirportId: Int,
                           originCityId: Int,
                           destAirportId: Int,
                           destCityId: Int,
                           arrivalDelay: Double) {

    def key(): FlightKey = {
      FlightKey(airLineId, destAirportId, arrivalDelay)
    }

  }


  object DelayedFlight {
    def parse(line: String): DelayedFlight = {
      val arr = line.split(",")
      DelayedFlight(arr(UNIQUE_CARRIER), arr(FL_DATE), safeInt(arr(ORIGIN_AIRPORT_ID)), safeInt(arr(ORIGIN_CITY_MARKET_ID)), safeInt(arr(DEST_AIRPORT_ID)), safeInt(arr(DEST_CITY_MARKET_ID)),safeDouble(arr(ARR_DELAY)))
    }

    def fromKeyAndData(k: FlightKey, l: List[String]): DelayedFlight = {
      DelayedFlight(k.airLineId,l(0),safeInt(l(1)), safeInt(l(2)),k.arrivalAirPortId,safeInt(l(3)),k.arrivalDelay)

    }
  }

  case class FlightKey(airLineId: String, arrivalAirPortId: Int, arrivalDelay: Double)

  object FlightKey {

    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
       Ordering.by(fk => (fk.airLineId, fk.arrivalAirPortId, fk.arrivalDelay * -1))
    }
  }

    class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[FlightKey]
      k.airLineId.hashCode() % numPartitions
    }
  }




}
