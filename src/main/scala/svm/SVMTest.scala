package svm

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/9/23.
	*/
object SVMTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("svm").setMaster("local")
		val sc = new SparkContext(conf)

		/**
			* 加载LibSVM格式数据
			*/
		//val data = MLUtils.loadLibSVMFile(sc, "file:///e://wa.txt")
		/**
			* 加载自定义数据格式进行转化
			*/
		val datas = sc.textFile("file:///e://svm2.txt")
		getdata(datas,1)
	}


	def getdata(datas:RDD[String],num:Int): Unit ={
		val nums = 100

		val data = datas.map { line =>
			val ports = line.split(",")
			val p = ports(3).split(" ")
			LabeledPoint(ports(num).toDouble,
				Vectors.dense(p.map(_.toDouble))
			)
		}
		val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
		val training = splits(0).cache()
		val test = splits(1)
		val model: SVMModel = SVMWithSGD.train(training, nums)


		val prediction = test.map { point =>
			val score = model.predict(point.features)
			(score, point.label)
		}
		val print_predict = prediction.take(10)
		println("prediction" + "\t" + "label")
		for (i <- 0 to print_predict.length - 1) {
			println(print_predict(i)._1 + "\t" + print_predict(i)._2)
		}
		val accury = 1.0 * prediction.filter(x => x._1 == x._2).count() / test.count()
		println("正确率：" + accury)
	}
	def gettest(): Unit ={

	}
}
