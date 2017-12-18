package com.its.demo.sparkstreaming.data

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class EventData(eventType: String, timestamp: String, errorData: String,
                     actualDepTime: String, CRSDepTime: String, arrTime: String, CRSArrTime: String, carrierCode: String, flightNumber: String,
                     tailNumber: String, actualElapsedTime: Int,
                     CRSElapsedTime: Int, airTime: Int, arrivalDelay: Int, departureDelay: Int, origin: String, destination: String,
                     distance: Int, taxiIn: Int, taxiOut: Int, cancelled: Int, cancellationCode: String, diverted: Int,
                     carrierDelay: Int, weatherDelay: Int, NASDelay: Int, securityDelay: Int, lateAircraftDelay: Int
                    ) extends Serializable {

    def eventDate(): String = { LocalDateTime.parse(timestamp).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) }
    def eventId (): String = { s"${actualDepTime}-${airTime}-${CRSArrTime}-${carrierCode}-${flightNumber}-${tailNumber}-${origin}-${destination}-${distance}"}

    def toJson() = {

        val jsonObjMapper = new ObjectMapper() with ScalaObjectMapper
        jsonObjMapper.registerModule(DefaultScalaModule)
        jsonObjMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        jsonObjMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
        jsonObjMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

        jsonObjMapper.writeValueAsString(this)
    }

}
