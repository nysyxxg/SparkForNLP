package svm

import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/9/23.
	*/
object SVMTest2 {
	def main(args: Array[String]): Unit = {
		//val conf = new SparkConf().setAppName("svm").setMaster("local")
		//val sc = new SparkContext(conf)
		val spark = SparkSession
			.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._

		/**
			* 加载LibSVM格式数据
			*/
		//val data = MLUtils.loadLibSVMFile(sc, "file:///e://wa.txt")
		/**
			* 加载自定义数据格式进行转化
			*/
		val datas = sc.textFile("file:///e://svm2.txt")
		val splits = datas.randomSplit(Array(0.8, 0.2), seed = 11L)
		val training = splits(0).cache()
		val test: RDD[String] = splits(1)
		gettest(test, getdata(training, test, 0), getdata(training, test, 1), getdata(training, test, 2))
	}

	def getdata(datas: RDD[String], tests: RDD[String], num: Int): RDD[String] = {
		val nums = 100

		val data = datas.map { line =>
			val ports = line.split(",")
			val p = ports(3).split(" ")
			LabeledPoint(ports(num).toDouble,
				Vectors.dense(p.map(_.toDouble))
			)
		}
		val test = tests.map { line =>
			val ports = line.split(",")
			val p = ports(3).split(" ")
			LabeledPoint(ports(num).toDouble,
				Vectors.dense(p.map(_.toDouble))
			)
		}

		val model = SVMWithSGD.train(data, nums)
		val predictlable = test.map { line =>
			model.predict(line.features).toString
		}
		println(num + "位置的预测值如下:")
		predictlable.foreach(println)
		return predictlable
	}

	def gettest(tests: RDD[String], prelable1: RDD[String], prelable2: RDD[String], prelable3: RDD[String]): Unit = {
		//	def gettest(tests:RDD[String],k:RDD[String]): Unit ={
		val test = tests.map { line =>
			val ports = line.split(",")
			(ports(0).toDouble, ports(1).toDouble, ports(2).toDouble)
		}

		val le = prelable1.collect().length - 1
		val p1 = prelable1.take(le)
		val p2 = prelable2.take(le)
		val p3 = prelable3.take(le)
		val t4 = test.take(le)


		println("prediction" + "\t" + "label")
		for (i <- 0 to 2) {
			println((p1(i), p2(i), p3(i)) + "\t" + t4(i))
		}

		//val accury = 1.0 * test.filter(x => x._1 == x._2).count() / tests.count()
	///	println("正确率：" + accury)
	}
}
