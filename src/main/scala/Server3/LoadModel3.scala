package Server3

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.{ml, mllib}

import scala.collection.JavaConversions._

/**
	* Created by MingDong on 2016/9/30.
	*/
object LoadModel3 {
	private val spark = SparkSession
		.builder().master("spark://master1:7077")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._

	case class RawDataRecords(lable: String, text: String)

	case class RawDataRecord(lable: String, message: Array[String])

	//加载tf-idf的model
	def tfidfModel(path: String): PipelineModel = {
		val model = PipelineModel.load(path)
		model
	}

	//加载SVM的model
	def svmModel(path: String): SVMModel = {
		val model = SVMModel.load(sc, path)
		model
	}

	//得到SVM的预测值
	def del(testDF: DataFrame, model: SVMModel): String = {
		val DataRdd = testDF.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val pres = MllibDataRdd.map { point =>
			val score = model.predict(point.features)
			score.toString
		}
		val p: String = pres.collect().toSeq.get(0)
		p
	}

	//得到SVM的预测值
	def del2(testDF: DataFrame, model: SVMModel): RDD[String] = {
		val DataRdd = testDF.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val pres = MllibDataRdd.map { point =>
			val score = model.predict(point.features)
			score.toString
		}
		pres
	}

	//批处理预测（准确率会低）

	def pric(str: String, tfidf1: PipelineModel, tfidf2: PipelineModel, svm1: SVMModel, svm2: SVMModel): java.util.List[String] = {

		val RDDs = sc.parallelize(str.split("@@"))
		val testRDD = RDDs.map { x =>
			RawDataRecords("0", x)
		}
		val tDF = spark.createDataFrame(testRDD).toDF()
		//(1)加载自定义词典（向原有补充新词,可加载多个）（后续考虑去重）
		sc.textFile("hdfs://master1:8020/svm/dic.txt")
			.map(_.split("	")).collect()
			.foreach(attributes => UserDefineLibrary.insertWord(attributes(0), attributes(1), 1000))
		//(2)加载停用词词表
		val stopwords: java.util.List[String] = sc.textFile("hdfs://master1:8020/svm/stopword.dic").collect().toSeq
		FilterModifWord.insertStopWords(stopwords)
		//(3)根据词性去停用词
		//FilterModifWord.insertStopNatures("w", null)

		//零件四：分词
		val testDF: DataFrame = tDF.select("text").map { case Row(text: String) =>
			//分词
			val temp = ToAnalysis.parse(text)
			val filter = FilterModifWord.modifResult(temp)
			val words = for (i <- Range(0, filter.size())) yield filter.get(i).getName
			val w = words.mkString(" ") //词之间以空格分割
			RawDataRecord("0",w.split(" "))
			//w
		}.toDF()
		testDF.show()

		//对于是否中立进行预测：
		//(预测tfidf)
		val td1 = tfidf1.transform(testDF)
		//(预测中性是或否(1.0 or 0.0))d
		val pre1 = del2(td1, svm1)

		//对于正负进行预测：
		//(预测tfidf)
		val td2 = tfidf2.transform(testDF)
		//(预测正或负(1.0 or 0.0))
		val pre2 = del2(td2, svm2)
		val count = pre1.collect().length
		val p1 = pre1.take(count)
		val p2 = pre2.take(count)
		val p = new Array[String](count)
		for (i <- 0 to (count - 1)) {
			//	p.update(i, (p1(i),p2(i)).toString())
			if (p1(i) == "1.0") {
				p.update(i, ("0"))
			} else {
				if (p2(i) == "0.0") {
					p.update(i, ("-1"))
				} else {
					p.update(i, ("1"))
				}
			}
		}
	//	p.foreach(println)
		p.toSeq
	}

}
