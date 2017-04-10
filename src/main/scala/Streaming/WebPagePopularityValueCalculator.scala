package Streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
	* Created by MingDong on 2017/2/15.
	*/
object WebPagePopularityValueCalculator {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
		val ssc = new StreamingContext(conf, Seconds(10))
		val topics = Set("test")
		val brokers = "slave4:9092,slave5:9092"
		val kafkaParams = Map[String, String](
			"metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

		// Create a direct stream
		val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, kafkaParams, topics)

		kafkaStream.print(10)
		ssc.start()
		ssc.awaitTermination()

	}

}
