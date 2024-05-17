package handlers

import Candlestick
import ISIN
import TimedQuoteEvent
import exceptions.TimeBracketMismatchException
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Queue
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.collections.HashMap
//----------------------CONSTANTS----------------------------

val INTERVAL:Int = 1 // in minutes
val Q_CAPACITY: Int = INTERVAL*60

//----------------------UTILITY FUNCTIONS--------------------
fun timestampToKey(i:Instant): String{
    // encodes an instant's hour and minute to a string for usage like a key
    // eg 08:35 ->0835
    val dateNTime = i.toString().split("T")
    val onlyTime = dateNTime[1].split(":")
    val hrsNMinutes = onlyTime[0]+onlyTime[1]
    return hrsNMinutes
}

fun getOpeningTimestampForInterval(openingTimestamp: Instant): Instant {
    // returns the official ending interval for the given opening timestamp based on the interval bracket
    return openingTimestamp.truncatedTo(ChronoUnit.MINUTES)
}

fun getClosingTimestampAfterInterval(intervalMinutes: Long, openingTimestamp: Instant): Instant{
    // returns the official ending interval for the given opening timestamp based on the interval bracket
    val roundFloor = getOpeningTimestampForInterval(openingTimestamp)
    val closingTimestamp = roundFloor.plus(intervalMinutes, ChronoUnit.MINUTES)
    return closingTimestamp
}

internal object Compare {
    // compares two TimedQuoteEvents by the price of the quote
    fun min(a: TimedQuoteEvent, b: TimedQuoteEvent): TimedQuoteEvent {
        return if (a.data.price < b.data.price) a else b
    }

    fun max(a: TimedQuoteEvent, b: TimedQuoteEvent): TimedQuoteEvent {
        return if (a.data.price > b.data.price) a else b
    }
}

//----------------------ENDOF UTILITY FUNCTIONS--------------------

// handlers.EventAggregatorQueue queues quotes for an isin in buckets for that particular minute
// On the next minute, we flush the current minute records into a single candlestick
class EventAggregatorQueue(

    private var minuteID: String = timestampToKey(Instant.now()),
    @Volatile private var list : BlockingQueue<TimedQuoteEvent> = LinkedBlockingQueue(Q_CAPACITY)
){
    private val logger = LoggerFactory.getLogger("EventAggregatorQueue")

    fun add(event: TimedQuoteEvent){
      val current = event.time
      val key = timestampToKey(current)

      if(!this.minuteID.equals(key))
          throw TimeBracketMismatchException()

      list.add(event)
  }

    fun getLastEvent(): TimedQuoteEvent?{
        return list.last()
    }

    fun toCandlestick(): Candlestick? {
        if (list.isEmpty())
            return null
        val openEvent = list.peek()
        val closeEvent = list.last()
        val openTimestamp = getOpeningTimestampForInterval(openEvent.time)
        val closeTimestamp = getClosingTimestampAfterInterval(INTERVAL.toLong(),openTimestamp)
        val openPrice = openEvent.data.price
        val closePrice = closeEvent.data.price
        val high = list.reduce(Compare::max).data.price
        val low = list.reduce(Compare::min) .data.price
        return Candlestick(openTimestamp, closeTimestamp, openPrice, high, low, closePrice)
    }

    fun reset(instant: Instant){
        if (!this.list.isEmpty())
            this.list.clear()
        this.minuteID = timestampToKey(instant)
    }

    fun flushAndReset(instant : Instant): Candlestick?{
        val candlestick = this.toCandlestick()
        this.reset(instant)
        return candlestick
    }

    fun peekContents():List<TimedQuoteEvent>{
        return list.toList()
    }
}

// This Singleton EventAggregator manages the handlers.EventAggregatorQueue for every isin and can be called at the server level
object EventAggregator{

    private val logger = LoggerFactory.getLogger("EventAggregatorQueue")
    @Volatile private var db: ConcurrentHashMap<String, EventAggregatorQueue> = ConcurrentHashMap()

    fun getQueueFor(isin: String): EventAggregatorQueue{
        return db.getOrDefault(isin,  EventAggregatorQueue())
    }

    fun getCurrentQueue(isin: ISIN): EventAggregatorQueue?{
        return db.get(isin)
    }

    fun clearRecordsForInstrument(isin : ISIN){
        // any unlisted instrument will have its queued records cleared
        if(db.get(isin)!=null)
            db.remove(isin)
    }

    fun add(cm: CandlestickManagerImpl, event: TimedQuoteEvent){
        // this function assumes all previous validations of isin and that the isin is added in the registry

        val isin = event.data.isin
        // only update valid Instruments
        if(!cm.contains(isin)) {
            logger.warn("Cannot add OutOfOrder ISIN. Missing ISIN {} from candlestickManager", isin)
            return
        }

        val queue = getQueueFor(isin)
        try {
            // accumulate the event in the current minute bracket
            queue.add(event)
        }
        catch (e: TimeBracketMismatchException){
            // if this is a new minute, flush the current candlestick
            val toNewMinuteBracket = event.time
            // and set the queue to this new timebracket
            val currentCandlestick = queue.flushAndReset(toNewMinuteBracket)
            // now save the candlestick to the candlestickManager
            if(currentCandlestick!=null)
                // cm
                cm.addCandlestick(isin,currentCandlestick)


            queue.add(event)
        }
        // persist the new points in the db
        db.put(isin, queue)
    }
}