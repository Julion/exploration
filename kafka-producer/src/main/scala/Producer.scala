import java.time.LocalDateTime
import java.util

import TradingPhases.TradingPhases
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

object Producer extends App {



  val topic: String = "marketdata"
  val random = scala.util.Random
  val volatility = 0.05


  produce()
  Thread.sleep(1000)
  //consume()

  def produce() {
    val producer = KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:9092")
    )

    println("sending data...")


    val numberOfTicks = 10000
    val endOfStartOfDayAuctionPhase = 500
    val startOfEndOfDayAuctionPhase = 9500

    val numberOfSecurities = 10

    def generateBasePrices: Map[Int, Double] = {
      var lastPrices: Map[Int, Double] = Map()

      var secId = 0
      for (secId <- 0 to numberOfSecurities)
        lastPrices += (secId -> random.nextDouble() * 50)

      lastPrices
    }

    var lastPrices = generateBasePrices

     (1 to numberOfTicks).foreach(tick => {
       val inAuction = tick <= endOfStartOfDayAuctionPhase || tick >= startOfEndOfDayAuctionPhase
       val phase = if (inAuction) TradingPhases.Auction else TradingPhases.Continuous
       val securityId = random.nextInt(numberOfSecurities)
       val lastPrice = lastPrices(securityId)
       val newPrice = getRandomPrice(lastPrice)

       lastPrices += (securityId -> newPrice)

       val md = new MarketData(securityId, LocalDateTime.now(), newPrice, phase)

       val str = f"${md.stockId};${md.time};${md.last}%.2f;${md.tradingPhase}" // bid, ask, volume...
       Thread.sleep(10)

       println(str)
       val record = new ProducerRecord[String, String](topic, md.stockId.toString, str)
       producer.send(record)
     })

    //val lastPrices = new Map[Int, Int]()

    println("data sent... closing!")
  }

  def getRandomPrice(basePrice: Double): Double = {

    val rnd = random.nextFloat()
    var change_percent = 2 * volatility * rnd
    if (change_percent > volatility)
      change_percent -=  (2 * volatility)
    val change_amount = basePrice * change_percent
    basePrice + change_amount
  }

  def consume(): Unit = {
    val consumer = KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:9092", groupId = "group")
    )

    val topics = util.Arrays.asList(topic)

    consumer.subscribe(topics)

    for (i <- 1 to 5) {
      println(s"Loop ${i}")

      val records = consumer.poll(250).asScala
      val marketData = records.map(rec => MarketData.deserialize(rec.value))

      marketData.zipWithIndex foreach { e =>
        val (md, i) = e
        println(s"${i}: ${md.stockId};${md.time};${md.last%1.2f};${md.tradingPhase}")
      }

    }

    println("data received... closing!")

  }
}