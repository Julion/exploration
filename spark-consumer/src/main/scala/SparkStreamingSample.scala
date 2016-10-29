import java.time.ZoneOffset
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object SparkStreamingSample {

  val topic: String = "marketdata"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => MarketData.deserialize(record.value))
      .map(md => ((md.stockId, md.tradingPhase, md.time.toEpochSecond(ZoneOffset.UTC)), md.last))
      .reduceByKeyAndWindow(_ + _, Seconds(1))
      .print()


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
