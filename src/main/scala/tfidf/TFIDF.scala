/*package tfidf

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

/**
	* Created by MingDong on 2016/9/28.
	*/
object TFIDF {
	val spark = SparkSession
		.builder().master("spark://192.168.1.109:7077")
		.config("spark.sql.warehouse.dir", "park-warehouse")
		.getOrCreate()
	val sc = spark.sparkContext

	import spark.implicits._

	//TF-IDF函数定义代码
	def termDocWeight(termFrequencyInDoc: Int, totalTermsInDoc: Int,
										termFreqInCorpus: Int, totalDocs: Int): Double = {
		val tf = totalTermsInDoc.toDouble / totalTermsInDoc
		val docFreq = totalDocs.toDouble / termFreqInCorpus
		val idf = math.log(docFreq)
		tf * idf
	}

	//构建一个此项数据RDD
	val lemmatized = sc.textFile("file:///e:/tfidf.txt").map { line =>
		line.split(" ")
	}

	//构建文档的词项频率的映射
	val docTermFreqs = lemmatized.map(terms => {
		val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
			(map, terms) => {
				map += terms -> (map.getOrElse(terms, 0) + 1)
				map
			}
		}
		termFreqs
	})
	docTermFreqs.cache()
	//后面会用到，所以缓存起来

	//计算文档频率
	val zero = new HashMap[String, Int]()

	def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int]): HashMap[String, Int] = {
		tfs.keySet.foreach { term =>
			dfs += term -> (dfs.getOrElse(term, 0) + 1)
		}
		dfs
	}

	def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int]): HashMap[String, Int] = {
		for ((term, count) <- dfs2) {
			dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
		}
		dfs1
	}
	docTermFreqs.aggregate(zero)(merge, comb)

	val docFreqs = docTermFreqs.flatMap(_.keySet).map((_,1)).reduceByKey(_+_)

	val numTerms = 5000;
	val ordering = Ordering.by[(String,Int),Int](_._2)
	val topDocFreqs = docFreqs.top(numTerms)(ordering)
	val idfs = docFreqs.map{
		case (term,count) => (term,math.log(numDocs.toDouble / count))
	}.toMap
	val termIds = idfs.keys.zipWithIndex.toMap

	val bTermIds = sc.broadcast(termIds).value


	//为每个文档简历一个含权重的TF-IDF向量（稀疏向量）
	val vecs = docTermFreqs.map(termFreqs =>{
		val docTotalTerms = termFreqs.values.sum
		val termScores = termFreqs.filter{
			case (term,freq) => bTermIds.containsKey(term)
		}.map{
			case (term,freq) => (bTermIds(term),
				bIdfs(term) * termFreqs(term) / docTotalTerms
				)
		}.toSeq
		Vectors.sparse(bTermIds.size,termScores)
	})
	def main(args: Array[String]): Unit = {
		println(docTermFreqs.flatMap(_.keySet).distinct().count())
	}
}*/
