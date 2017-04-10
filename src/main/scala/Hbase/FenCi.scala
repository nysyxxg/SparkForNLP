package Hbase

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
object FenCi {
	def main(args: Array[String]): Unit = {
		//零件一：初始化环境
		//(1)Spark
		val spark = SparkSession
			.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._



		//零件二：获取数据
		val hbaseRDD = sc.textFile("file:///e:/svm/luntanzhengfu.txt")

		//零件三：加载分词配置
		//(1)加载自定义词典（向原有补充新词,可加载多个）（后续考虑去重）
		sc.textFile("file:///e://test.txt")
			.map(_.split(" ")).collect()
			.foreach(attributes => UserDefineLibrary.insertWord(attributes(0), attributes(1), 1000))
		//	sc.textFile("file:///e://out.txt").collect().foreach(UserDefineLibrary.insertWord(_, "userDefine", 1000))
		sc.textFile("file:///e://default.dic").map{line=>
			val part = line.split("	")
			UserDefineLibrary.insertWord(part(0),part(1),part(2).toInt)
		}
		//(2)加载停用词词表
		val stopwords: java.util.List[String] = sc.textFile("file:///e://stopword.dic").collect().toSeq
		FilterModifWord.insertStopWords(stopwords)
		//(3)根据词性去停用词
		FilterModifWord.insertStopNatures("w", null)

		//零件四：分词
		val data: DataFrame = hbaseRDD.map { line =>
			val p = line.split(",")
			val content = p(1)
			(p(0), {
				//去除文章内容中的Html标签
				val text = droHtml(content)
				//分词
				val temp = ToAnalysis.parse(text)
				val filter = FilterModifWord.modifResult(temp)
				val words = for (i <- Range(0, filter.size())) yield filter.get(i).getName //.replaceAll(" ","")
				words.mkString(" ") //词之间以制表符分割
			}
				)
		}.toDF("lable", "words")
		//零件五：打印前5条预览
		data.select("lable", "words").take(2).foreach(println)
	data.select("lable","words").rdd.saveAsTextFile("file:///e://svm//luntanzhengfufenci/")
	/*	//零件六：转为TF-IDF
		var tokenizer = new Tokenizer().setInputCol("words").setOutputCol("word")
		var wordsData = tokenizer.transform(data)
		wordsData.select("word").take(5).foreach(println)

		val hashingTF = new HashingTF()
			.setInputCol("word")
			.setOutputCol("rawFeatures")
			.setNumFeatures(10000)
		var featurizedData = hashingTF.transform(wordsData)

		val idf = new IDF()
			.setInputCol("rawFeatures").setOutputCol("features")
		val idfModel = idf.fit(featurizedData)
		val rescaledData = idfModel.transform(featurizedData)
		rescaledData.select("features").take(3).foreach(println)
	//rescaledData.select("features").collect().foreach(Row =>)*/
	}


	//去除文本中的Html标签
	def droHtml(str: String): String = {
		var doc = Jsoup.parse(str).text()
		val builder = new StringBuilder(doc)
		var index: Int = 0
		while (builder.length > index) {
			val tmp: Char = builder.charAt(index)
			if (Character.isSpaceChar(tmp) || Character.isWhitespace(tmp)) {
				builder.setCharAt(index, ' ')
			}
			index = index + 1
		}
		doc = builder.toString().replaceAll(" +", " ").trim()
			.replaceAll(" ", "")
		doc
	}
}
