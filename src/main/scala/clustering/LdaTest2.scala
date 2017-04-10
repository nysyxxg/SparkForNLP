package clustering

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.SparkSession

/**
	* Created by MingDong on 2017/3/24.
	* 使用TF-IDF值作为LDA聚类输入
	*/
object LdaTest2 {
	private val spark = SparkSession
		.builder()
		.config("spark.sql.warehouse.dir", "IdeaProjects/sparktest/spark-warehouse")
		.master("local[2]")
		.getOrCreate()
	private val sc = spark.sparkContext

	//private var i:Long = -1
	case class RawData(xuhao: Int,lable: String, message: Array[String])
	def main(args: Array[String]): Unit = {
		val datas = sc.textFile("file:///e://fenlei//").map(_.split(" ").toSeq)
		//val datas = sc.textFile("hdfs://mycluster/Mdsd-bigdata-DPMS/Resource/Corpus/Classify/").map(_.split(" ").toSeq)
		//datas.foreach(println)
		//println("*****documets文本****")
		//println("****文本编号****")
		//datas.zipWithIndex.foreach(println)
		val termCounts = datas.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
		//println("****单词计数****")
		//termCounts.foreach(println)
		val d = termCounts.map(_._1)
		val hashingTF = new HashingTF()
		val tf = hashingTF.transform(datas)
		//tf.foreach(println)
	//	println(hashingTF.indexOf("spark"))
		//println(hashingTF.indexOf("hello"))
		//println(hashingTF.indexOf("goodbye"))
		//println(hashingTF.indexOf("mllib"))
		var d2:Map[Int,String]=Map()
		d.map{x=>
			val counts = new scala.collection.mutable.HashMap[Int, String]()
		val i = hashingTF.indexOf(x)
			d2+=(i->x)
		}
		//d2.foreach(println)
		//println(d2)
		val idf = new IDF().fit(tf)

		val tf_idf = idf.transform(tf)

		//tf_idf.foreach(println)

		val t = tf_idf.zipWithIndex.map{
			case (v,l)=>
				(l,v)
		}
t.foreach(println)
		val lda = new LDA().setK(3).setMaxIterations(5)
		val ldaModel = lda.run(t)
		val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)

		//topicIndices.foreach(println)

		topicIndices.foreach { case (terms, termWeights) =>

				//terms.foreach(println)
			//	termWeights.foreach(println)
			 println("TOPIC:")
			terms.zip(termWeights).foreach { case (term, weight) =>
				println(s"${d2(term.toInt)}\t$weight")
			}
		}
		sc.stop()
	}
}
