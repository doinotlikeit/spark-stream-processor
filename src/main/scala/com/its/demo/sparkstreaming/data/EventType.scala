package com.its.demo.sparkstreaming.data

object EventType extends Enumeration  {
    type TxnType = Value

    val GOOD_DATA_REC = Value( "GOOD_DATA_REC")
    val ERROR_DATA_REC = Value ("ERROR_DATA_REC")
    val INGEST_TO_ES = Value("INGEST-TO-ES")

    val ERR_PROCESSING_COUNT = Value("ERR_PROCESSING_COUNT")
    val TOTAL_RCVD_COUNT = Value("TOTAL-RCVD-COUNT")

}