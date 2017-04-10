package clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

/**
	* Created by MingDong on 2017/3/29.
	* 使用词频作为输入进行LDA聚类
	*/
object LdaTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf
		conf.setAppName("App").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val hadoopRdd = sc.textFile("file:///E:\\fenlei\\")
		val dataRdd = hadoopRdd.map{x=>
		val d = x.split(",")
			d(1)
		}
		val documents: RDD[Seq[String]] = dataRdd.map(_.split(" ").toSeq)
		val termCounts = documents.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
		val d = termCounts.map(_._1)
		val vocab = d.zipWithIndex.toMap
		val vocabbc=sc.broadcast(vocab);//对vocab进行广播
		val documentss =documents.zipWithIndex.map {
			case (tokens, id) =>
				val counts = new scala.collection.mutable.HashMap[Int, Double]()
				tokens.foreach {
					term =>
						if (vocabbc.value.contains(term)) {
							val idx = vocabbc.value(term)
							counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
						}
				}
				(id, Vectors.sparse(vocab.size, counts.toSeq))
		}

		val lda = new LDA().setK(3).setMaxIterations(5)
		val ldaModel = lda.run(documentss)
		val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)


		topicIndices.foreach { case (terms, termWeights) =>
			println("TOPIC:")
			terms.zip(termWeights).foreach { case (term, weight) =>
				println(s"${d(term.toInt)}\t$weight")
			}
		}
		sc.stop()
	}
}
