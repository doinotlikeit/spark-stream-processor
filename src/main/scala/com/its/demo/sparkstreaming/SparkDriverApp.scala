package com.its.demo.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * =SparkDriverApp=
  *
  * Spark Driver Application.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

object SparkDriverApp extends App {

    val logger = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()
    var sparkCtx: SparkContext = null
    logger.info("===> Created the SparkContext ... ")

    if (args.length > 0) {
        args(0) match {
            case "local" => {
                sparkConf.setMaster("local[*]")
            }
        }
    }

    sparkConf
        .setAppName("Spark Streaming Demo")
        .set("kafka.bootstrap.servers", "localhost:9092")
        .set("kafka.input.topic", "input-data")
        .set("kafka.output.topic", "output-data")
        .set("spark.driver.maxResultSize", "3g")
        .set("spark.io.compression.codec", "snappy")
        .set("spark.rdd.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.kryoserializer.buffer.max", "128m")
        .set("spark.executor.logs.rolling.time.interval", "hourly")
        .set("spark.executor.logs.rolling.maxRetainedFiles", "1")
        .set("spark.executor.logs.rolling.enableCompression", "true")
        .set("es.index.auto.create", "false")
        .set("es.nodes", "localhost")

    var writeToEs = true
    if (args.length > 1 && args.find(_ == "noWriteToEs").isDefined) {
        writeToEs = false
    }

    var writeToOutTopic = true
    if (args.length > 1 && args.find(_ == "noWriteToKafka").isDefined) {
        writeToOutTopic = false
    }

    System.setProperty("es.nodes", sparkConf.get("es.nodes"))
    System.setProperty("kafka.bootstrap.servers", sparkConf.get("kafka.bootstrap.servers"))
    System.setProperty("kafka.input.topic", sparkConf.get("kafka.input.topic"))

    // Start processing the event stream
    new SparkStreamProcessor(sparkConf, writeToEs, writeToOutTopic).startProcessing()

}