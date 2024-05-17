import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import handlers.CandlestickManagerImpl
import handlers.EventAggregator
import handlers.EventCleaningThread
import handlers.InstrumentRegistry
import org.slf4j.LoggerFactory
import java.time.Instant

private val logger = LoggerFactory.getLogger("MainKt")

fun main() {
  logger.info("starting up")

  val server = Server()
  val instrumentStream = InstrumentStream()
  val quoteStream = QuoteStream()

  // Singleton Storage and Queues that are shared across JVM
  val cm = CandlestickManagerImpl
  val eventManager = EventAggregator
  val instrumentRegistry = InstrumentRegistry

  instrumentStream.connect { event ->

    logger.info("Instrument to be {}'D: {}",event.type,event.data )
    // ADDS or DELETES event in the CandlestickManager and InstrumentRegistry
    val isin = event.data.isin
    if(event.type.equals(InstrumentEvent.Type.DELETE)) {
      instrumentRegistry.deregisterInstrument(isin,cm)
    }
    else if(event.type.equals(InstrumentEvent.Type.ADD)) {
      instrumentRegistry.registerInstrument(event.data,cm)
    }

  }

  quoteStream.connect { event ->

    logger.info("Quote: {}", event)
    // eventManager aggregates events per minute against every VALID instrument
    // and flushes them into a persisting Candlestick in the storage in the next minute
    eventManager.add(cm,TimedQuoteEvent(event.data, Instant.now()))
  }

  server.start()

  //You can run another server to enhance performance as this is threadsafe
  /*val server2 = Server(9001)
  server2.start()*/

  // you can run a cronjob to collect events >1min that haven't been compacted to candlesticks yet
  /*val cleaner = Thread(EventCleaningThread())
  cleaner.start()*/

}

val jackson: ObjectMapper =
  jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
