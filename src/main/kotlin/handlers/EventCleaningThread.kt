package handlers

import java.lang.Thread.sleep
import java.time.Instant
import java.time.temporal.ChronoUnit

class EventCleaningThread: Runnable {

    // runs every INTERVAL minutes to compact Instrument's candlesticks
    override fun run() {

        while(true){

            val now = Instant.now()
            for(isin in InstrumentRegistry.getInstrumentIDs()){
                val q = EventAggregator.getQueueFor(isin)
                val lastEvent = q.getLastEvent()
                if (lastEvent==null)
                    continue
                val durationBetween = ChronoUnit.MINUTES.between(lastEvent.time, now)
                // cleanup of any Instruments which have last quote reported > minute ago
                if(durationBetween >= INTERVAL){
                    val candlestick = q.toCandlestick()
                    q.reset(now)
                    if(candlestick!=null)
                        CandlestickManagerImpl.addCandlestick(isin,candlestick)
                }
            }

            sleep(2*INTERVAL*60*1000L)
        }
    }
}