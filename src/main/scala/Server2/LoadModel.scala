package Server2

import java.util

import org.apache.spark.ml.PipelineModel
import org.apache.spark.{ml, mllib}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, _}
import org.dmg.pmml.Segmentation

import scala.collection.JavaConversions._

/**
	* Created by MingDong on 2016/9/30.
	*/
object LoadModel {
	private val spark = SparkSession
		.builder()
		.config("spark.sql.warehouse.dir", "spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._

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
			RawDataRecord("0", x.split(" "))
		}
		println("数据转换成RDD、DF")
		testRDD.collect().foreach(println)
		val testDF = spark.createDataFrame(testRDD).toDF()

		println("开始预测中性")

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
		//val p3 = prediction3.take(count)
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
		p.foreach(println)
		p.toSeq
	}



	//预测
	def pric2(str: String, tfidf1: PipelineModel, tfidf2: PipelineModel, svm1: SVMModel, svm2: SVMModel): String = {
		val RDDs = sc.parallelize(Seq(str))
		val testRDD = RDDs.map { x =>
			RawDataRecord("0", x.split(" "))
		}
		Segmentation.del(testRDD)

		val testDF = spark.createDataFrame(testRDD).toDF()

		//对于是否中立进行预测：
		//(预测tfidf)
		val td1 = tfidf1.transform(testDF)
		//(预测中性是或否(1.0 or 0.0))
		val pre1 = del(td1, svm1)
		if (pre1 == "1.0") {
			"0"
		} else {
			//对于正负进行预测：
			//(预测tfidf)
			val td2 = tfidf2.transform(testDF)
			//(预测正或负(1.0 or 0.0))
			val pre2 = del(td2, svm2)
			if (pre2 == "0.0") {
				"-1"
			}
			else {
				"1"
			}
		}
	}
}
