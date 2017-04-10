package svm

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
	* Created by MingDong on 2016/9/23.
	*/
object SVMTest3 {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._

		//加载数据
		val datas2 = sc.textFile("file:///e://svm3.txt").map { x =>
			var data = x.split(":")
			//(data(0).split(",") ,data(1).split(" "))
			(data(0), data(1))

		}
		//将数据分开转化为RDD
		val dataDF = spark.createDataFrame(datas2).toDF("lables", "messages")
		val lables = dataDF.select("lables").map { case Row(lable: String) =>
			lable
		}.rdd
		val messages = dataDF.select("messages").map { case Row(message: String) =>
			message
		}.rdd

		val tokenizer = messages.map(_.split(" ").toSeq)


		//计算TF-IDF
		val hashingTF = new HashingTF()
		val tf = hashingTF.transform(tokenizer)
		val idf = new IDF().fit(tf)
		val tfidf = idf.transform(tf)
    tfidf.foreach(println)
		//训练SVM模型
/*

	val da = lables.map{line => val p = line.split(",")
	p(0)
	}.zip(tfidf).foreach(println)
*/
	}


/*
	def getdata(datas: RDD[String], tests: RDD[String], num: Int): RDD[String] = {
		val nums = 100

		val data = datas.map { line =>
			val ports = line.split(":")
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
		for (i <- 0 to 5) {
			println((p1(i), p2(i), p3(i)) + "\t" + t4(i))
		}

		val accury = 1.0 * test.filter(x => x._1 == x._2).count() / tests.count()
		println("正确率：" + accury)
	}*/
}
