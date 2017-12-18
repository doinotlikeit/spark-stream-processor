package com.its.demo.sparkstreaming

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}
import com.its.demo.sparkstreaming.data.{EventDataHandler, EventType}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * =SparkStreamProcessor=
  *
  * Process the event stream from the Kafka Topic.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

class SparkStreamProcessor(sparkConf: SparkConf, isWriteToES: Boolean, writeToOutTopic: Boolean) extends Serializable {

    val logger = LoggerFactory.getLogger(this.getClass)
    var startTime = LocalDateTime.now()


    def startProcessing() = {

        // Create a StreamingContext with a specified batch interval
        val streamingCtx: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

        val dStream: DStream[ConsumerRecord[String, String]] = init(streamingCtx)


        try {

            val eventDataHandler = EventDataHandler.getInstance(streamingCtx.sparkContext).value
            val incrementCounts = JMXStatsManager().initInstance(streamingCtx.sparkContext)
            val elasticsearchIngester = streamingCtx.sparkContext.broadcast(ElasticsearchIngester(System.getProperty("es.nodes")))
            val kafkaCommitOffsetCallback = (offsetRanges: Array[OffsetRange]) => {
                dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            }
            val kafkaIngester = streamingCtx.sparkContext.broadcast(
                KafkaTopicIngester(
                    System.getProperty("kafka.bootstrap.servers"),
                    System.getProperty("kafka.output.topic")
                )
            )

            // Process each RDD received in the DStream
            dStream.foreachRDD(consumerRecRDD => {

                // Consumed Kafka offset range
                val kafkaOffsetRanges = consumerRecRDD.asInstanceOf[HasOffsetRanges].offsetRanges

                // Process each Spark partition; Kafka and Spark partitions have a 1:1 relationship
                consumerRecRDD.foreachPartition(consumerRecordPartition => {

                    // Data records received from Kafka are encapsulate in a ConsumerRecord
                    consumerRecordPartition.foreach(consumerRecord => {

                        // Process an input data record, receive the converted JSON payload as an EventData object
                        var eventDataRecord = consumerRecord.value()
                        if (eventDataRecord.contains("payload")) {
                            val startIdx = eventDataRecord.indexOf("\"payload\":\"") + 11
                            val endIdx = eventDataRecord.indexOf("\"}", startIdx)
                            if (endIdx != -1) {
                                eventDataRecord = eventDataRecord.substring(startIdx, endIdx)
                            } else {
                                eventDataRecord = "Rcvd bad data"
                            }
                        }
                        val eventDataObj = eventDataHandler.process(eventDataRecord)
                        if (eventDataObj.eventType == EventType.ERROR_DATA_REC.toString)
                            incrementCounts(EventType.ERR_PROCESSING_COUNT)

                        // Ingest to Elasticsearch
                        if (isWriteToES == true) elasticsearchIngester.value.ingest(eventDataObj, incrementCounts)

                        // Ingest to the ooutput Kafka Topic
                        if (writeToOutTopic == true) kafkaIngester.value.ingestData(eventDataObj)

                        incrementCounts(EventType.TOTAL_RCVD_COUNT)
                    })
                })

                // Commit consumed offset in order to delete what's successfully consumed from the Kafka log
                kafkaCommitOffsetCallback(kafkaOffsetRanges)
            })

            streamingCtx.start()
            logger.info("===> Started processing, waiting for events from Kafka ...")
            streamingCtx.awaitTermination()
            logger.info("===> Stopped processing ...")

        } catch {
            case e: Throwable => {
                throw new IllegalStateException("Couldn't start processing", e)
            }
        }
    }

    /**
      * Connect to the target to Kafka Brokers and reate the Spark Streaming DStream
      */
    def init(streamingContext: StreamingContext): DStream[ConsumerRecord[String, String]] = {

        var SparkDStream: DStream[ConsumerRecord[String, String]] = null
        var subscribeTopics = Array[String]()

        logger.info(s"===> Write to Kafka: $writeToOutTopic, write to ES: $isWriteToES")

        sys.addShutdownHook {
            logger.info("===> Shutting down ...")
            val endTime = LocalDateTime.now()
            streamingContext.stop()
            val df = DateTimeFormatter.ISO_LOCAL_TIME
            val elapsedTime = java.time.Duration.between(startTime, endTime)
            logger.info(s"********* Total time: " + s"${LocalTime.ofNanoOfDay(elapsedTime.toNanos()).format(df)}\n")
            logger.info(s"***********************************************************************************************************")
            logger.info(s"************* Done processing: ***************")
            logger.info(s"***********************************************************************************************************")
        }

        try {

            // Set up the input DStream to read from Kafka (in parallel)
            SparkDStream = {
                val kafkaParams = Map[String, Object](
                    "bootstrap.servers" -> System.getProperty("kafka.bootstrap.servers"),
                    "key.deserializer" -> classOf[StringDeserializer],
                    "value.deserializer" -> classOf[StringDeserializer],
                    "group.id" -> "spark-streaming-demo",
                    "auto.offset.reset" -> "earliest",
                    "enable.auto.commit" -> "false")
                subscribeTopics = Array(System.getProperty("kafka.input.topic"))
                KafkaUtils.createDirectStream[String, String](
                    streamingContext,
                    LocationStrategies.PreferConsistent,
                    Subscribe[String, String](subscribeTopics, kafkaParams)
                )
            }
            logger.info(s"===> Kafka DStream created ...")

        } catch {
            case e: Throwable => {
                throw new IllegalStateException("Couldn't init Spark stream processing", e)
            }
        }

        logger.info(s"===> Connected to Kafka cluster: [${System.getProperty("kafka.bootstrap.servers")}], subscribed to: ${subscribeTopics.toList} ...")
        SparkDStream
    }

}