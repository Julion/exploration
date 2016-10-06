import java.time.LocalDateTime
import java.util

import TradingPhases.TradingPhases
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

object ScalaProducerExample extends App {

  produce()
  consume()

  def produce() {
    val producer = KafkaProducer(
      KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:9092")
    )

    println("sending data...")

    val random = scala.util.Random
    val numberOfTicks = 1000
    val endOfStartOfDayAuctionPhase = 25
    val startOfEndOfDayAuctionPhase = 950

    val marketData = (1 to numberOfTicks)
      .map(tick => {
        val inAuction = tick <= endOfStartOfDayAuctionPhase || tick >= startOfEndOfDayAuctionPhase
        val phase = if (inAuction) TradingPhases.Auction else TradingPhases.Continuous
        val md = new MarketData(random.nextInt(50), LocalDateTime.now(), random.nextDouble(), phase)
        val str = s"${md.id};${md.time};${md.last};${md.tradingPhase}"
        new ProducerRecord[String, String]("lol", str)
      })
      .foreach(producer.send(_))
    //val lastPrices = new Map[Int, Int]()

    println("data sent... closing!")
  }

  def consume(): Unit = {
    val consumer = KafkaConsumer(
      KafkaConsumer.Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:9092", groupId = "group")
    )

    val topics = util.Arrays.asList("lol")

    consumer.subscribe(topics)

    for (i <- 1 to 5) {
      println(s"Loop ${i}")

      val records = consumer.poll(250).asScala
      val marketData = records.map(rec => {
        val items = rec.value().split(";")
        MarketData(items(0).toInt, LocalDateTime.parse(items(1)), items(2).toDouble, TradingPhases.withName(items(3)))
      })

      marketData.foreach(md => println(s"${md.id};${md.time};${md.last}"))

    }

    println("data received... closing!")

  }



}

case class MarketData(id: Int, time: LocalDateTime, last: Double, tradingPhase: TradingPhases)