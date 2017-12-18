package com.its.demo.sparkstreaming

import java.net.InetAddress

import com.its.demo.sparkstreaming.data.{EventData, EventType}
import com.its.demo.sparkstreaming.data.EventType.TxnType
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory

/**
  * =ElasticsearchIngester=
  *
  * Use the Elasticsearch Bulk Processor to ingest JSON data received as an EventData objects.
  *
  * @author rajiv.cooray@itctcb.com
  * @since Dec 2017
  */

class ElasticsearchIngester(initEsFunction: () => BulkProcessor) extends Serializable {
    private val logger = LoggerFactory.getLogger(this.getClass)

    lazy val esBulkProcessor = initEsFunction()

    /**
      * Delegate indexingto the BulkProcessor to asynchronously write to Elasticsearch at the configured frequency.
      * <p />
      *
      * @param event                   - JSON data to ingest
      * @param incrementCountsCallback - Callback to invoke to keep a count of documents ingested to ES
      */
    def ingest(event: EventData, incrementCountsCallback: (TxnType) => Unit): Unit = {

        val indexName: String = s"data-${event.eventDate()}"
        //logger.info(s"===> Ingesting to: [${indexName}]")
        esBulkProcessor.add(new IndexRequest(indexName, event.eventType).source(event.toJson()))
        incrementCountsCallback(EventType.INGEST_TO_ES)
    }
}

object ElasticsearchIngester {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def apply(connectString: String): ElasticsearchIngester = {

        val initEsFunction = () => {
            var esClient: TransportClient = null
            var esBulkProcessor: BulkProcessor = null
            try {

                val settings: Settings = Settings.builder()
                    .put("cluster.name", "stream-data-demo")
                    .build()
                esClient = new PreBuiltTransportClient(settings)

                connectString.split(",").foreach(hostname => {
                    esClient.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300))
                })

                val callback: BulkProcessor.Listener = new Listener {
                    override def beforeBulk(l: Long, bulkRequest: BulkRequest) = {

                        //logger.info("===> Adding to ES ...")
                    }

                    override def afterBulk(l: Long, bulkRequest: BulkRequest, bulkResponse: BulkResponse) = {

                        //logger.info(s"===> Thread: [${Thread.currentThread().getName}] Successfully added to ES ...")
                    }

                    override def afterBulk(l: Long, bulkRequest: BulkRequest, throwable: Throwable) = {

                        logger.error("===> Error Adding to ES", throwable)
                    }
                }

                esBulkProcessor = BulkProcessor.builder(esClient, callback)
                    .setBulkActions(5000)
                    .setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(10)
                    .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build()

                sys.addShutdownHook {
                    esBulkProcessor.flush()
                    logger.info("===> Flushing and closing ...")
                }

                logger.info("===> Created the Elasticsearch Bulk Processor ...")

            } catch {
                case e: Throwable => {
                    throw new IllegalStateException(s"Couldn't connect to Elasticsearch with the configuration: [${System.getProperty("es.nodes")}]", e)
                }
            }
            logger.info("*************************************************************************************************************************")
            logger.info(s"******************* Successfully connected to the Elasticsearch Cluster: [${connectString}] ********************")
            logger.info("*************************************************************************************************************************")
            val qb: QueryBuilder = QueryBuilders.matchAllQuery()
            val searchResponse = esClient.prepareSearch("data-*").setQuery(qb).execute().actionGet()
            logger.info(s"===> Total docs: ${searchResponse.getHits.totalHits}")
            esBulkProcessor
        }

        new ElasticsearchIngester(initEsFunction)
    }
}

