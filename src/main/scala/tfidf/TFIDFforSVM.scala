package tfidf

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/9/28.
	*/
object TFIDFforSVM {
	val keywordMap = scala.collection.mutable.Map(1 -> "name")

	def nonNegativeMod(x: Int, mod: Int): Int = {
		val rawMod = x % mod
		rawMod + (if (rawMod < 0) mod else 0)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf
		conf.setAppName("App").setMaster("local")
		val sc = new SparkContext(conf)
		//    val hadoopRdd = sc.textFile("hdfs://localhost:9000/spark-in/1.txt")
		val hadoopRdd = sc.textFile("file:///e:/tfidf.txt")
		// Load documents (one per line).
		val documents: RDD[Seq[String]] = hadoopRdd.map(_.split(" ").toSeq)
		documents.foreach(w => {
			for(v <- w){
				keywordMap += (nonNegativeMod(v.##, 1 << 20) -> v)
			}
		})
		val hashingTF = new HashingTF()
		val tf: RDD[linalg.Vector] = hashingTF.transform(documents)
		tf.cache()
		val idf = new IDF().fit(tf)
		val tfidf: RDD[linalg.Vector] = idf.transform(tf)
		val tfidfOfName = tfidf.partitions

		val mapFromRdd = tfidf.map(line => (line.toSparse.indices -> line.toSparse.values))

		val customerMapRDD = mapFromRdd.map(
			line => {
				val changeToName = (x: Int) => keywordMap.getOrElse(x,0)
				val kList = line._1.map(changeToName)
				val nameMap = kList.toList.zip(line._2.toList)
				nameMap
			}

		)

		println("-----------------")
		customerMapRDD.foreach(println)
		sc.stop()
	}
}
