package tfidf

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/9/28.
	*/
object TFIDFtest {
	def nonNegativeMod(x: Int, mod: Int): Int = {
		val rawMod = x % mod
		rawMod + (if (rawMod < 0) mod else 0)
	}

	//  def main(args: Array[String]) {
	//
	//    println(nonNegativeMod("ls".##, 1 << 20))
	//    println(1 << 20)
	//  }
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf
		conf.setAppName("App").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val hadoopRdd = sc.textFile("file:///E:\\tfidf.txt")
		print("*****原始文本*****")
		hadoopRdd.foreach(println)
		println("*****documets文本****")
		val documents: RDD[Seq[String]] = hadoopRdd.map(_.split(" ").toSeq)
		documents.foreach(println)
		println("****文本编号****")
		documents.zipWithIndex.foreach(println)
		val termCounts = documents.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
		println("****单词计数****")
		termCounts.foreach(println)
		val d = termCounts.map(_._1)
		println("****所有词（词袋）****")
		d.foreach(println)
		val vocab = d.zipWithIndex.toMap
		println("词编号"+vocab+vocab.size)
		val vocabbc=sc.broadcast(vocab)//对vocab进行广播
		println("广播词编号"+vocabbc.value)
		val documentss =documents.zipWithIndex.map {
			case (tokens, id) =>       //(WrappedArray(hello, mllib),0)
				val counts = new scala.collection.mutable.HashMap[Int, Double]()
				tokens.foreach {
					term => //hello
						if (vocabbc.value.contains(term)) { //Map(spark -> 0, hello -> 1, goodbye -> 2, mllib -> 3)
							val idx = vocabbc.value(term) // hello.value:1
							counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
						}
				}
				counts.foreach(println)
				println(counts.toSeq)
				(id, Vectors.sparse(vocab.size, counts.toSeq))
		}


		documentss.foreach(println)
		val lda = new LDA().setK(2).setMaxIterations(20)
		val ldaModel = lda.run(documentss)
		val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)


		topicIndices.foreach { case (terms, termWeights) =>
			println("TOPIC:")
			terms.zip(termWeights).foreach { case (term, weight) =>
				println(s"${d(term.toInt)}\t$weight")
			}
		}
		/*documents.foreach(w => {
			for(v <- w){
				print(v)
				print("=")
				println(v.##)
			}
		})*/
		sc.stop()
	}
}
