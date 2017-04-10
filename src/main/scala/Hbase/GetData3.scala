package Hbase

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jsoup.Jsoup
import org.nlpcn.commons.lang.tire.domain.Forest

import scala.collection.JavaConversions._

/**
	* Created by MingDong on 2016/8/23.
	*/
object GetData3 {
	val f: Forest = null

	def main(args: Array[String]): Unit = {

		val hconf = HBaseConfiguration.create()
		hconf.set(TableInputFormat.INPUT_TABLE, "test")
		hconf.set(TableInputFormat.SCAN_ROW_START, "1466870400000")
		hconf.set(TableInputFormat.SCAN_ROW_STOP, "147421440665000")

		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		// 从数据源获取数据
		val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
		//hbaseRDD.foreach(println)

		//对表中的数据项进行分词
		//val text = hbaseRDD.map(r => Bytes.toString(r._2.getValue(Bytes.toBytes("article"), Bytes.toBytes("title")))) //得到数据的内容RDD

		// 将数据映射为表  也就是将 RDD转化为 dataframe schema
		//UserDefineLibrary.insertWord("咨询服务工作","userDefine", 1000)  //加词典
		sc.textFile("file:///e://test.txt")
			.map(_.split(" ")).collect().foreach(attributes => UserDefineLibrary.insertWord(attributes(0), attributes(1), 1000))
		//.collect().foreach(UserDefineLibrary.insertWord(_,"",1000))
		//.map(word => UserDefineLibrary.insertWord(word,"userDefine", 1000))
		sc.textFile("file:///e://out.txt").collect().foreach(UserDefineLibrary.insertWord(_, "", 1000))
		val li = sc.textFile("file:///e://stopword.txt").collect()
		//val l: List[String] = Seq("的","我是")
		val l: java.util.List[String] = li.toSeq
					FilterModifWord.insertStopWords(l)
					//加入停用词性
					FilterModifWord.insertStopNatures("w",null)
		val data: DataFrame = {
			hbaseRDD.map(r => (Bytes.toString(r._1.get()), //得到rowKey
				{
					val temps = Bytes.toString(r._2.getValue(Bytes.toBytes("article"), Bytes.toBytes("title")))
					val text = proHtml(temps)
					val temp = ToAnalysis.parse(text)
					val filter = FilterModifWord.modifResult(temp)
					val key = for (i <- Range(0, filter.size())) yield filter.get(i).getName//.replaceAll(" ","")
					key.mkString("\t")
				} //得到某一列数据
				)).toDF("id", "content")
		}
		data.select("id", "content").take(8).foreach(println) //打印前8条数据\
	}
	def proHtml(str:String): String ={
		var doc = Jsoup.parse(str).text()
		val builder = new StringBuilder(doc)
		var index:Int = 0
		while (builder.length > index) {
			val tmp:Char = builder.charAt(index)
			if (Character.isSpaceChar(tmp) || Character.isWhitespace(tmp)) {
				builder.setCharAt(index, ' ')
			}
			index = index+1
		}
		doc = builder.toString().replaceAll(" +", " ").trim()
			.replaceAll(" ", "")
		 doc

	}
}
