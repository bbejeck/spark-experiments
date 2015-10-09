package bbejeck

import org.apache.spark.Partitioner

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

  //Lookup keys
  val AIRLINE_DATA = "L_UNIQUE_CARRIERS"
  val AIRPORT_DATA = "L_AIRPORT_ID"
  val CITY_DATA = "L_CITY_MARKET_ID"

  case class DelayedFlight(airLine: String,
                           date: String,
                           originAirport: String,
                           originCity: String,
                           destAirport: String,
                           destCity: String,
                           arrivalDelay: Double) {
  }


  case class FlightKey(airLineId: String, arrivalAirportId: Int, arrivalDelay: Double)

  object FlightKey {

    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
       Ordering.by(fk => (fk.airLineId, fk.arrivalAirportId, fk.arrivalDelay * -1))
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
