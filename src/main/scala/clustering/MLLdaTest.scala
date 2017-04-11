package clustering

import org.apache.spark.ml
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
	* Created by MingDong on 2017/3/30.
	*/
object MLLdaTest {
	private val spark = SparkSession
		.builder()
		.config("spark.sql.warehouse.dir", "IdeaProjects/sparktest/spark-warehouse")
		.master("local[2]")
		.getOrCreate()
	private val sc = spark.sparkContext
	import spark.implicits._
	case class RawData(xuhao:Long, text: String)
	case class RawFaeu(features:ml.linalg.Vector)
	def main(args: Array[String]): Unit = {
		val data = sc.textFile("file:///E://fenlei//").map{x=> x.split(",")(1)}
		data.take(5).foreach(println)
		//println("****单词计数****")
		val termCounts = data.map(_.split(" ").toSeq).flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
		//println("****所有的词组****")
		val d = termCounts.map(_._1)

		val datas = data.zipWithIndex().map{case(text,i)=>
			RawData(i,text)
		}
		//data.foreach(println)
		val msgDF = spark.createDataFrame(datas).toDF()
		msgDF.show(5)
		//将词语按空格切分转换成数组
		var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		var wordsData = tokenizer.transform(msgDF)
		wordsData.show(5)
		//计算每个词在文档中的词频
		val hashingTF = new HashingTF()
		val tf = hashingTF.transform(data.map(_.split(" ").toSeq))
		val idf = new IDF().fit(tf)

		val tf_idf = idf.transform(tf)

		//tf_idf.foreach(println)

		val t = tf_idf.zipWithIndex.map{
			case (v,l)=>
				(l,v)
		}
		val t2 = t.map(_._2).map{
			x=>
			RawFaeu(	ml.linalg.Vectors.dense(x.toArray))
		}
		//t2.foreach(println)
		val fea = spark.createDataFrame(t2).toDF("features")
		//println(hashingTF)
		var d2:Map[Int,String]=Map()
		d.map{x=>
			val counts = new scala.collection.mutable.HashMap[Int, String]()
			val i = hashingTF.indexOf(x)
			d2+=(i->x)
		}
		//println(d2)
		fea.show(5)
		val model = new LDA().setK(3).setMaxIter(5).fit(fea)
		val topics=model.describeTopics(maxTermsPerTopic=5)
		topics.show(5)
		topics.select("termIndices").map{
			case Row(termIndices:Seq[Int])=>
				termIndices.map{x=>
				d2(x)
				}
		}.show(5)
	}
}
