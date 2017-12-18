package com.its.demo.sparkstreaming.data

import java.time.LocalDateTime

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

/**
  * =EventDataHandler=
  *
  * Process a record of data received from Kafka, convert it to a JSON stream, and return as an EventData object.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

object EventDataHandler extends Serializable {

    private val logger = LoggerFactory.getLogger(this.getClass)


    @volatile private var instance: Broadcast[EventDataHandler.type] = null


    def getInstance(ssc: SparkContext): Broadcast[EventDataHandler.type] = {

        synchronized {
            if (instance == null) {
                instance = ssc.broadcast(this)
            }
        }
        instance
    }

    def process(eventDataRecord: String): EventData = {

        var eventData: EventData = null
        var year: Int = 0
        var month: Int = 0
        var dayOfMonth: Int = 0
        var dayOfWeek: Int = 0
        var actualDepTime: String = null
        var CRSDepTime: String = null
        var arrTime: String = null
        var CRSArrTime: String = null
        var carrierCode: String = null
        var flightNumber: String = null
        var tailNumber: String = null
        var actualElapsedTime: Int = 0
        var CRSElapsedTime: Int = 0
        var airTime: Int = 0
        var arrivalDelay: Int = 0
        var departureDelay: Int = 0
        var origin: String = null
        var destination: String = null
        var distance: Int = 0
        var taxiIn: Int = 0
        var taxiOut: Int = 0
        var cancelled: Int = 0
        var cancellationCode: String = null
        var diverted: Int = 0
        var carrierDelay: Int = 0
        var weatherDelay: Int = 0
        var NASDelay: Int = 0
        var securityDelay: Int = 0
        var lateAircraftDelay: Int = 0

        val createErrorEventDataInstance = (errorData: String) => {
            val timeStamp: LocalDateTime = LocalDateTime.of(LocalDateTime.now().getYear, LocalDateTime.now().getMonth, LocalDateTime.now().getDayOfMonth, 0, 0)
            eventData = new EventData(EventType.ERROR_DATA_REC.toString, timeStamp.toString, errorData, null, null,
                null, null, null, null, null, 0, 0, 0,
                0, 0, null, null, 0, 0, 0, 0, null,
                0, 0, 0, 0, 0, 0)
        }

        try {

            val splitted = eventDataRecord.split(',')

            splitted.zipWithIndex.foreach { case (item: String, idx: Int) =>

                if ( idx == 0 && !StringUtils.isNumeric(item)) {
                    createErrorEventDataInstance("Received invalid data; record does not start with numeric data")
                } else {

                    idx match {
                        case 0 => year = Integer.parseInt(item)
                        case 1 => month = Integer.parseInt(item)
                        case 2 => dayOfMonth = Integer.parseInt(item)
                        case 3 => dayOfWeek = Integer.parseInt(item)
                        case 4 => actualDepTime = item
                        case 5 => CRSDepTime = item
                        case 6 => arrTime = item
                        case 7 => CRSArrTime = item
                        case 8 => carrierCode = item
                        case 9 => flightNumber = item
                        case 10 => tailNumber = item
                        case 11 => actualElapsedTime = if (item == "NA") 0 else Integer.parseInt(item)
                        case 12 => CRSElapsedTime = if (item == "NA") 0 else Integer.parseInt(item)
                        case 13 => airTime = if (item == "NA") 0 else Integer.parseInt(item)
                        case 14 => arrivalDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 15 => departureDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 16 => origin = item
                        case 17 => destination = item
                        case 18 => distance = if (item == "NA") 0 else Integer.parseInt(item)
                        case 19 => taxiIn = if (item == "NA") 0 else Integer.parseInt(item)
                        case 20 => taxiOut = if (item == "NA") 0 else Integer.parseInt(item)
                        case 21 => cancelled = if (item == "NA") 0 else Integer.parseInt(item)
                        case 22 => cancellationCode = if (item == "") "NA" else item
                        case 23 => diverted = Integer.parseInt(item)
                        case 24 => carrierDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 25 => weatherDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 26 => NASDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 27 => securityDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case 28 => lateAircraftDelay = if (item == "NA") 0 else Integer.parseInt(item)
                        case _ =>
                    }

                    //println(s"${idx} => ${item}")

                    if (year != 0 && month != 0 && dayOfWeek != 0) {
                        val timeStamp: LocalDateTime = LocalDateTime.of(year, month, dayOfMonth, 0, 0)
                        eventData = new EventData(EventType.GOOD_DATA_REC.toString, timeStamp.toString, null, actualDepTime, CRSDepTime,
                            arrTime, CRSArrTime, carrierCode, flightNumber, tailNumber, actualElapsedTime, CRSElapsedTime, airTime,
                            arrivalDelay, departureDelay, origin, destination, distance, taxiIn, taxiOut, cancelled, cancellationCode,
                            diverted, carrierDelay, weatherDelay, NASDelay, securityDelay, lateAircraftDelay)
                        logger.info(s"===> JSON payload: ${eventData.toJson()}")
                    } else {
                        createErrorEventDataInstance("Couldn't determine the timestamp; invalid data rcvd")
                    }
                }
            }
        }
        catch {
            case e: Throwable => {
                logger.error("Couldn't process the event data record rcvd", e)
                createErrorEventDataInstance(e.toString)
            }
        }

        eventData
    }
}
