package handlers

import Candlestick
import CandlestickManager
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.LinkedHashMap

// create a queue per Instrument per minute and compact it to a Candlestick at the end of the minute

object CandlestickManagerImpl : CandlestickManager{
    // This is a Singleton Storage where we orgainze ISIN's candlesticks per minuteBracket
    // 1st level gives you all the candlesticks for the ISIN for max of last 24 hrs
    // 2nd level gives you the candlestick for that minute
    // The maximum candlesticks that we are storing is for the last 24 hrs
    // We use a LinkedHashMap here to mantain the order of insertion which is analogus to time of insertion
    // Hence to get last 30 candlesticks, we can simply return last 30 entries in the hashmap

    private val db:ConcurrentHashMap<String, LinkedHashMap<String,Candlestick>> = ConcurrentHashMap()
    private val logger = LoggerFactory.getLogger("CandlestickManager")

    fun reset(){
        // recycles the memory in case you want to restart from scratch
        this.db.clear()
    }

    fun setEventAggregator(mockQueue: EventAggregator){

    }

    fun removeInstrument(isin: String){
        if (contains(isin)) {
            db.remove(isin)
        }
        // volatile records should be deleted
        EventAggregator.clearRecordsForInstrument(isin)
    }

    fun contains(isin : String): Boolean{
        return db.containsKey(isin)
    }

    fun addInstrument(isin: String){
        if (!contains(isin)) {
            db.put(isin, LinkedHashMap())
        }
    }

    fun addCandlestick(isin: String, candle: Candlestick){
        // we need to only save last 30min candlesticks
        val timeBracket = timestampToKey(candle.openTimestamp)
        val entry = db.get(isin)
        entry?.put(timeBracket,candle)

        if (entry!=null && entry.size>30) {
            // truncate the size of the hashmap to at most last 30 recs
            val firstEntry = entry.entries.iterator().next()
            entry.remove(firstEntry.key)
        }

    }

    override fun getCandlesticks(isin: String): List<Candlestick>{
        val result = db.get(isin)?.values?.toList() ?: ArrayList()
        return result
    }
}