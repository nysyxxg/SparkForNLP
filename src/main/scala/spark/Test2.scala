package spark

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.interactive.Lexer.Token

/**
	* Created by MingDong on 2016/8/22.
	*/
object Test2 {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().set("spark.shuffle.blockTransferService", "nio")
		val spark = SparkSession
			.builder.config(conf).master("spark://master1:7077").config("deploy-mode","client")
			.appName("PipelineExample")
			.config("spark.sql.warehouse.dir", "hdfs://master1:8020/spark-warehouse")
			.getOrCreate()

		// $example on$
		// Prepare training documents from a list of (id, text, label) tuples.
		val training = spark.createDataFrame(Seq(
			(0L, "a b c d e spark", 1.0),
			(1L, "b d", 0.0),
			(2L, "spark f g h", 1.0),
			(3L, "hadoop mapreduce", 0.0)
		)).toDF("id", "text", "label")
		val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
		var wordsData = tokenizer.transform(training)
		println(wordsData)
		wordsData.select("id","text","word").take(4).foreach(println)

		var hashingTF = new HashingTF().setNumFeatures(20)
			.setInputCol("word").setOutputCol("raw")
		var featur = hashingTF.transform(wordsData)
		featur.select("id","word","raw").take(4).foreach(println)
	}
}
