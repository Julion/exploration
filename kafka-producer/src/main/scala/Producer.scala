import java.time.LocalDateTime
import java.util

import TradingPhases.TradingPhases
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

object Producer extends App {

  produce()
  Thread.sleep(1000)
  //consume()

  val topic: String = "marketdata"

  def produce() {
    val producer = KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:9092")
    )

    println("sending data...")

    val random = scala.util.Random
    val numberOfTicks = 10000
    val endOfStartOfDayAuctionPhase = 25
    val startOfEndOfDayAuctionPhase = 950

    val marketData = (1 to numberOfTicks)
      .map(tick => {
        val inAuction = tick <= endOfStartOfDayAuctionPhase || tick >= startOfEndOfDayAuctionPhase
        val phase = if (inAuction) TradingPhases.Auction else TradingPhases.Continuous
        val md = new MarketData(random.nextInt(50), LocalDateTime.now(), random.nextDouble(), phase)
        val str = s"${md.stockId};${md.time};${md.last};${md.tradingPhase}" // bid, ask, volume...
        Thread.sleep(10)

        new ProducerRecord[String, String](topic, md.stockId.toString, str)
      })
      .foreach(producer.send)
    //val lastPrices = new Map[Int, Int]()

    println("data sent... closing!")
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
        println(s"${i}: ${md.stockId};${md.time};${md.last};${md.tradingPhase}")
      }

    }

    println("data received... closing!")

  }
}