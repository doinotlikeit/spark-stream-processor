package com.its.demo.sparkstreaming

import com.its.demo.sparkstreaming.data.EventData

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

/**
  * =KafkaTopicIngester=
  *
  * Use the Kafka Producer client to ingest JSON data received as an EventData objects to a target Kafka Topic.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

class KafkaTopicIngester(initProducerFunction: () => KafkaProducer[String, String], targetTopicName: String) extends Serializable {
    private val logger = LoggerFactory.getLogger(this.getClass)

    lazy val kafkaProducerInstance = initProducerFunction()
    val this.`targetTopicName` = targetTopicName
    val numOfPartitions = 5

    def ingestData(event: EventData): Unit = {

        try {

            kafkaProducerInstance.send(new ProducerRecord[String, String](targetTopicName, event.eventId, event.toJson()))
            logger.info(s"===> Event: [${event.eventId}] written to topic: [$targetTopicName], data: ${event.toJson()}")

        } catch {
            case e: Throwable => {
                logger.error(s"Couldn't write to the target Kafka Topic: [$targetTopicName]", e)
            }
        }
    }
}

object KafkaTopicIngester {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def apply(bootstrapServers: String, topicName: String): KafkaTopicIngester = {

        val initFunction = () => {
            var producerInstance: KafkaProducer[String, String] = null
            try {

                val props = Map(
                    "bootstrap.servers" -> bootstrapServers,
                    "acks" -> "all",
                    "retries" -> "0",
                    "batch.size" -> "16384",
                    "linger.ms" -> "1",
                    "buffer.memory" -> "33554432",
                    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
                )
                producerInstance = new KafkaProducer[String, String](JavaConversions.mapAsJavaMap[String, Object](props))
                sys.addShutdownHook {
                    producerInstance.close()
                    logger.info("===> Stopped the Kafka Producer ...")
                }
                logger.info("===> Created the Kafka Producer ...")

                logger.info(s"===> Successfully connected to the Kafka Cluster Cluster: [${bootstrapServers}], output Topic: [${topicName}]")

            } catch {
                case e: Throwable => {
                    throw new IllegalStateException(s"Couldn't connect to the target Kafka Cluster: [$bootstrapServers], output Topic: [${topicName}", e)
                }
            }
            producerInstance
        }
        new KafkaTopicIngester(initFunction, topicName)
    }
}

