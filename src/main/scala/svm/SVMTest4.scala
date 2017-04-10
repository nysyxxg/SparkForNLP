package svm

import java.util

import org.apache.spark.{ml, mllib}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{LabeledPoint, _}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
	* 用work2Vec来向量化数据做特征完成的SVM情感分类
	* Created by MingDong on 2016/9/1.
	*/
object SVMTest4 {

	case class RawDataRecord(label: String, message: Array[String])

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local[2]")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val parsedRDD = sc.textFile("file:///e:/svm3.txt").map {
			x =>
				val data = x.split(":")
				RawDataRecord(data(0), data(1).split(" "))
		}

		val msgDF = spark.createDataFrame(parsedRDD).toDF("label", "message")
		val lables = msgDF.select("label")
		val labelIndexer = new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(lables)

		val word2Vec = new Word2Vec()
			.setInputCol("message")
			.setOutputCol("features")
			.setVectorSize(100)
			.setMinCount(0)
		//val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.3), seed = 11L)
		val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec))
		val model = pipeline.fit(msgDF)
		val predictionResultDF = model.transform(msgDF)
		predictionResultDF.printSchema
		predictionResultDF.select("label", "features").show(10)

		var DataRdd = predictionResultDF.select($"label", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.split(",")(0).toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		DataRdd.foreach(println)
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea.map(_.toDouble)))
		}
		val Array(training,test) = MllibDataRdd.randomSplit(Array(0.9,0.1),seed = 11L)

		val model2 = SVMWithSGD.train(training,100)

		//对测试数据集使用训练模型进行分类预测
		val prediction = test.map { point =>
			val score = model2.predict(point.features)
			(score, point.label)
		}
		val print_predict = prediction.take(20)
		println("prediction" + "\t" + "label")
		for (i <- 0 to print_predict.length - 1) {
			println(print_predict(i)._1 + "\t" + print_predict(i)._2)
		}
		//统计分类准确率
		val accury = 1.0 * prediction.filter(x => x._1 == x._2).count() / test.count()
		println("正确率：" + accury)
		/*		val data = datas.map { line =>
					LabeledPoint(line._1.toDouble,
						Vectors.dense(line._2)
					)
				}
					val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
					val training = splits(0).cache()
					val test = splits(1)
					val model2: SVMModel = SVMWithSGD.train(training, 100)


					val prediction = test.map { point =>
						val score = model2.predict(point.features)
						(score, point.label)
					}
					val print_predict = prediction.take(10)
					println("prediction" + "\t" + "label")
					for (i <- 0 to print_predict.length - 1) {
						println(print_predict(i)._1 + "\t" + print_predict(i)._2)
					}
					val accury = 1.0 * prediction.filter(x => x._1 == x._2).count() / test.count()
					println("正确率：" + accury)*/
	}
}