package com.its.demo.sparkstreaming

import com.codahale.metrics.{Gauge, JmxReporter, MetricRegistry}
import com.its.demo.sparkstreaming.data.EventType
import com.its.demo.sparkstreaming.data.EventType.TxnType
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
/**
  * =JMXStatsManager=
  *
  * Report processsing stats via JMX.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

object JMXStatsManager {
    def apply(): JMXStatsManager = {

        new JMXStatsManager()
    }
}

class JMXStatsManager() extends Serializable {

    var totalRcvdCount: LongAccumulator = null
    var errorProcessingCount: LongAccumulator = null
    var esIngestCount: LongAccumulator = null


    val newLongAccumulator = (ssc: SparkContext, registry: MetricRegistry, description: String) => {
        val accumulator = ssc.longAccumulator(description)
        registry.register(accumulator.name.get, new Gauge[Long]() {
            override def getValue: Long = {

                accumulator.value
            }
        })
        accumulator
    }

    def incrementESIngestCount(): Unit = {

        esIngestCount.add(1)
    }

    def initInstance(sparkCtx: SparkContext) = {

        val registry = new MetricRegistry()
        JmxReporter.forRegistry(registry).build().start()

        totalRcvdCount = newLongAccumulator(sparkCtx, registry, "TOTAL RCVD")
        errorProcessingCount = newLongAccumulator(sparkCtx, registry, "TOTAL ERROR PROCESSING COUNT")
        esIngestCount = newLongAccumulator(sparkCtx, registry, "Elasticsearch ingest Count")

        val callBack = (txnType: TxnType) => {
            txnType match {
                case EventType.ERR_PROCESSING_COUNT => {
                    errorProcessingCount.add(1)
                }
                case EventType.INGEST_TO_ES => {
                    esIngestCount.add(1)
                }
                case EventType.TOTAL_RCVD_COUNT => {
                    totalRcvdCount.add(1)
                }
            }
        }

        callBack
    }

}


