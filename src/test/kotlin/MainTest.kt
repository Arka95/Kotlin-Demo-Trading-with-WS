import handlers.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class MainTest {

  fun eventOf(s :String): TimedQuoteEvent{
    val arguments = s.split(" ")
    val date = arguments[0]
    val time = arguments[1]
    val price:Price = arguments[3].trim().toDouble()

    val isin:ISIN =arguments[4]
    val ldt = LocalDateTime.parse(date+'T'+time)
    val i: Instant = ldt.toInstant(ZoneOffset.UTC)
    return TimedQuoteEvent(Quote(isin,price), i)
  }

  fun prepareData():List<TimedQuoteEvent>{

    val testdata  = arrayOf("2019-03-05 13:00:05 price: 10",
    "2019-03-05 13:00:06 price: 11",
    "2019-03-05 13:00:13 price: 15",
    "2019-03-05 13:00:19 price: 11",
    "2019-03-05 13:00:32 price: 13",
    "2019-03-05 13:00:49 price: 12",
    "2019-03-05 13:00:57 price: 12",
    "2019-03-05 13:01:00 price: 9")
    val randomname =  "ABCD"+(Math.random()%1000).toString()
    val quoteEvents = testdata.toList().map {  x->eventOf(x+" "+randomname)}.toList()
    return quoteEvents
  }

  fun prepareInstruments(): List<Instrument>{
   val names = arrayOf("ABCD","EFGH", "IJKL", "MNOP", "QRST")
    return names.toList().map {  name->Instrument(name,"This is a description of this awesome instrument")}.toList()
  }

  @Test
  fun testGetClosingTimestampAfterInterval(){
    // test corner cases for rounding off time based on interval
    val interval = INTERVAL.toLong()
    val events = prepareData()
    // basic opening time test
    var opening = events[0].time
    var closing = getClosingTimestampAfterInterval(interval,opening)
    assert(closing.toString().contains("13:01:00"))
    // borderline opening time is also rounder off
    opening = events[6].time
    assert(getOpeningTimestampForInterval(opening).toString().contains("13:00:00"))
    assert(getClosingTimestampAfterInterval(interval.toLong(),opening).toString().contains("13:01:00"))
    // this one should be new window with new closing time
    opening = events[7].time
    assert(getClosingTimestampAfterInterval(interval,opening).toString().contains("13:02:00"))
  }

  @Test
  fun testInstrumentRegistry(){
    val ins = prepareInstruments()
    val registry = InstrumentRegistry
    val cm = CandlestickManagerImpl
    assert(!cm.contains(ins.get(0).isin))
    registry.registerInstrument(ins.get(0), cm)
    assert(registry.contains(ins.get(0).isin))
    assert(cm.contains(ins.get(0).isin))
    assertNotNull(cm.getCandlesticks(ins.get(0).isin))
    registry.deregisterInstrument(ins.get(0).isin, cm)
    assert(!registry.contains(ins.get(0).isin))
    assert(!cm.contains(ins.get(0).isin))
    // cleanup
    registry.reset()
    cm.reset()
  }


  @Test
  fun testAggregatorBasedOnReadme() {
    val events = prepareData()
    val ea = EventAggregator
    val cm = CandlestickManagerImpl
    val assetname = events.get(0).data.isin

    cm.addInstrument(assetname)
    assert(cm.contains(assetname))
    for(event:TimedQuoteEvent in events ){
      ea.add(cm, event)
    }
    assertEquals(1, cm.getCandlesticks(assetname).size)

    val currentCandleStick = cm.getCandlesticks(assetname).get(0)
    assert(currentCandleStick.openTimestamp.toString().contains("13:00:00"))
    assertEquals(10.0,currentCandleStick.openPrice)
    assertEquals(15.0,currentCandleStick.highPrice)
    assertEquals(10.0,currentCandleStick.lowPrice)
    assertEquals(12.0,currentCandleStick.closingPrice)
    assert(currentCandleStick.closeTimestamp.toString().contains("13:01:00"))

    val ensureCorrectBracketForNextCandle= ea.getQueueFor(assetname).peekContents().size
    assertEquals(1, ensureCorrectBracketForNextCandle)

    // test Candlestick for empty queue is a Null object
    ea.getQueueFor(assetname).reset(Instant.now())
    assertNull(ea.getQueueFor(assetname).toCandlestick())

    // test EventAggregator does not add events for unregistered ISINS (OOO ISINS)
    cm.removeInstrument(assetname)
    ea.add(cm,events.get(1))
    assertNull(ea.getCurrentQueue(assetname))
  }
  
}
