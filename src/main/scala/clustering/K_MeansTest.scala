package clustering

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.sql.SparkSession

/**
	* Created by MingDong on 2017/4/7.
	*/
object K_MeansTest {
	private val spark = SparkSession
		.builder()
		.config("spark.sql.warehouse.dir", "IdeaProjects/sparktest/spark-warehouse")
		.master("local[2]")
		.getOrCreate()
	private val sc = spark.sparkContext

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
		val hashingTF = new HashingTF(d.length)
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

		val numClusters = 3
		val numIterations = 80
		val clusters = KMeans.train(tf_idf, numClusters, numIterations)
		// 输出每个子集的质心
		clusters.clusterCenters.foreach { println }

		val kMeansCost = clusters.computeCost(tf_idf)
		// 输出本次聚类操作的收敛性，此值越低越好
		println("K-Means Cost: " + kMeansCost)

		// 输出每组数据及其所属的子集索引
		tf_idf.foreach {
			vec =>
				println(clusters.predict(vec) + ": " + vec)
		}

	}
}
