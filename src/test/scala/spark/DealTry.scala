package spark

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jsoup.Jsoup

import scala.collection.JavaConversions._

/**
	* Created by MingDong on 2016/8/29.
	*/
object DealTry {
	def main(args: Array[String]): Unit = {
		//零件一：初始化环境
		//(1)Spark
		val spark = SparkSession
			.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		val r = sc.textFile("file:///e://default.dic").map{line=>
			val part = line.split("	")
			UserDefineLibrary.insertWord(part(0),part(1),part(2).toInt)
		}
		//(_.split("	")).foreach{attributes => UserDefineLibrary.insertWord(attributes(0), "", 10000)}

		//r.foreach(line => println(line(0)+"--"+line(1)+"--"+line(2).toInt))

	}
}
