package svm

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{ml, mllib}

/**
	* 用work2Vec来向量化数据做特征完成的SVM情感分类
	* Created by MingDong on 2016/9/1.
	*/
object SVMTest4Plus {

	private val spark = SparkSession
		.builder().master("local[2]")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._


	case class RawDataRecord(lable: String, message: Array[String])

	def main(args: Array[String]): Unit = {

		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val parsedRDD = sc.textFile("file:///e:/svm4.txt").map {
			x =>
				val data = x.split(":")
				RawDataRecord(data(0), data(1).split(" "))
		}

		val msgDF = spark.createDataFrame(parsedRDD).toDF()
		val Array(training1, test1) = msgDF.randomSplit(Array(0.8, 0.2), seed = 11L)

		val socres = test1.select($"lable") //.rdd.collect()
			.map { case Row(lable: String) =>
			"(" + lable + ")"
		}.rdd.cache()

		val model = getModel1(training1)
		//向量化语料集
		val training2 = model.transform(training1)
		//向量化测试集
		val test2 = model.transform(test1)

		//得到是否正面的SVMmodel
		val smodel1 = getModel2(training2, 0)
		//得到是否正面的预测值
		val prediction1 = TestModel(smodel1, test2, 0)

		//得到是否中立面的SVMmodel
		val smodel2 = getModel2(training2, 1)
		//得到是否中立面的预测值
		val prediction2 = TestModel(smodel2, test2, 1)

		//得到是否负面的SVMmodel
		val smodel3 = getModel2(training2, 2)
		//得到是否负面的预测值
		val prediction3 = TestModel(smodel3, test2, 2)
		val count = prediction1.collect().length
		val p1 = prediction1.take(count)
		val p2 = prediction2.take(count)
		val p3 = prediction3.take(count)
		val p = new Array[String](count)
		for (i <- 0 to (count - 1)) {
			if (p1(i) == 1.0) {
				p.update(i, (1, 0, 0).toString())
			} else if (p2(i) == 1.0) {
				p.update(i, (0,1,0).toString())
			} else {
				p.update(i, (0,0,1).toString())
			}
		}
		println(count, prediction2.collect().length, prediction3.collect().length)
		println("预测值" + "\t" + "真实值")
		for (i <- 0 to (count - 1)) {
			println(p(i) + "\t" + socres.collect()(i))
		}
		var correct: Int = 0
		for (i <- 0 to (count - 1)) {
			if (socres.collect()(i).equals(p(i))) {
				correct = correct + 1
			}
		}
		println("正确个数是：" + correct)
		val accury = 1.0 * correct / test1.count()
		println("正确率是：" + accury)

		p.foreach(println)
		training1.select("lable").collect().foreach { case Row(la: String) =>
			println(la)
		}
	}

	//得到文本向量化的model
	def getModel1(dataDF: DataFrame): PipelineModel = {
		val lables = dataDF.select("lable")
		val labelIndexer = new StringIndexer()
			.setInputCol("lable")
			.setOutputCol("indexedLabel")
			.fit(lables)

		val word2Vec = new Word2Vec()
			.setInputCol("message")
			.setOutputCol("features")
			.setVectorSize(100)
			.setMinCount(0)
		//val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.3), seed = 11L)
		val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec))
		val model1 = pipeline.fit(dataDF)
		model1
	}

	//得到SVM分类器的model
	def getModel2(dataDF: DataFrame, num: Int): SVMModel = {
		val DataRdd = dataDF.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.split(",")(num).toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val model2 = SVMWithSGD.train(MllibDataRdd, 100)
		model2
	}

	//对最后的模型进行测试
	def TestModel(model: SVMModel, test: DataFrame, num: Int): RDD[Double] = {
		//model.clearThreshold()
		val DataRdd = test.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.split(",")(num).toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val pres = MllibDataRdd.map { point =>
			val score = model.predict(point.features)
			score
		}
		pres

	}
}