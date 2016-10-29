import java.time.LocalDateTime

import TradingPhases._

/**
  * Created by Julien on 28/10/2016.
  */
object MarketData {
  def deserialize(str: String): MarketData = {
    val items = str.split(";")
    MarketData(items(0).toInt, LocalDateTime.parse(items(1)), items(2).toDouble, TradingPhases.withName(items(3)))
  }


}

case class MarketData(stockId: Int, time: LocalDateTime, last: Double, tradingPhase: TradingPhases) {

}