package handlers

import ISIN
import Instrument
import java.util.concurrent.ConcurrentHashMap

object InstrumentRegistry {
    @Volatile private var db: ConcurrentHashMap<String, Instrument> = ConcurrentHashMap()

    fun registerInstrument(ins: Instrument, cm: CandlestickManagerImpl){
        val isin = ins.isin
        if (!contains(isin)) {
           db.put(isin, ins)
        }
        cm.addInstrument(isin)
    }

    fun deregisterInstrument(isin: ISIN, cm: CandlestickManagerImpl){
        if (contains(isin)) {
            db.remove(isin)
        }
        cm.removeInstrument(isin)
    }

    fun contains(isin : ISIN): Boolean{
        return db.containsKey(isin)
    }

    fun getInstrumentIDs(): List<ISIN>{
        return db.keys.toList()
    }

    fun getInstruments():List<Instrument>
    {
        return db.values.toList()
    }

    fun reset(){
        // recycles the memory in case you want to restart from scratch
        this.db.clear()
    }

}