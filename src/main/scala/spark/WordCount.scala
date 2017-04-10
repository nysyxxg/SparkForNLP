package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/8/22.
	*/
object WordCount {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
		val sc = new SparkContext(conf)

		val data = sc.textFile("file:///d://wc.txt")

		val result = data.flatMap(_.split(" ").map((_,1)))
		result.foreach(println)
	}
}
