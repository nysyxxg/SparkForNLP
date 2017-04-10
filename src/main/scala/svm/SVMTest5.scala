package svm

import breeze.linalg
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{ml, mllib}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
/**
	* 用TF-IDF值向量化数据来做特征完成的SVM情感分类
	* Created by MingDong on 2016/9/1.
	*/
object SVMTest5 {

	case class RawDataRecord(category: String, text: String)

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		/*	val conf = new SparkConf().setMaster("local").setAppName("test")
		val sc = new SparkContext(conf)

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			sqlContext.setConf("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		import sqlContext.implicits._*/

		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val srcRDD = sc.textFile("file:///e:/svm3.txt").map {
			x =>
				var data = x.split(":")
				RawDataRecord(data(0), data(1))
		}
		/*	srcDF.select("category", "text").take(2).foreach(println)
		var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		var wordsData = tokenizer.transform(srcDF)
		wordsData.select($"category",$"text",$"words").take(2).foreach(println)

		var hashingTF =
			new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
		var featurizedData = hashingTF.transform(wordsData)
		featurizedData.select($"category", $"words", $"rawFeatures").take(2).foreach(println)
		var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		var idfModel = idf.fit(featurizedData)
		var rescaledData = idfModel.transform(featurizedData)
		rescaledData.select($"category", $"words", $"features").take(2).foreach(println)

		var trainDataRdd = rescaledData.select($"category",$"features").map {
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}
		val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")*/

		val splits = srcRDD.randomSplit(Array(0.8, 0.2), seed = 11L)
		var trainingDF = splits(0).toDF()
		var testDF = splits(1).toDF()

		//将词语转换成数组
		var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		var wordsData = tokenizer.transform(trainingDF)


		//计算每个词在文档中的词频
		var hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("rawFeatures")
		var featurizedData = hashingTF.transform(wordsData)


		//计算每个词的TF-IDF
		var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		var idfModel = idf.fit(featurizedData)
		var rescaledData = idfModel.transform(featurizedData)

		//转换成Bayes的输入格式
		var trainDataRdd = rescaledData.select($"category", $"features").map {
			case Row(label: String, features:ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.split(",")(0).toDouble, ml.linalg.Vectors.dense(features.toArray))
				//(label,features)
		}.rdd
		trainDataRdd.foreach(println)
		val feauture = trainDataRdd.map{line => line.features.toArray}
		feauture.foreach(println)
		val da = trainDataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable,mllib.linalg.Vectors.dense(fea.map(_.toDouble)))
		}
		//训练模型
		da.foreach(println)
		val model = SVMWithSGD.train(da,100)

		//测试数据集，做同样的特征表示及格式转换
			var testwordsData = tokenizer.transform(testDF)
			var testfeaturizedData = hashingTF.transform(testwordsData)
			var testrescaledData = idfModel.transform(testfeaturizedData)

			var testDataRdd = testrescaledData.select($"category", $"features").map {
				case Row(label: String, features:ml.linalg.Vector) =>
					ml.feature.LabeledPoint(label.split(",")(0).toDouble, ml.linalg.Vectors.dense(features.toArray))
				//(label,features)
			}.rdd
		val testda = trainDataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable,mllib.linalg.Vectors.dense(fea.map(_.toDouble)))
		}
			//对测试数据集使用训练模型进行分类预测
			val prediction = testda.map { point =>
				val score = model.predict(point.features)
				(score, point.label)
			}
		val print_predict = prediction.take(10)
		println("prediction" + "\t" + "label")
		for (i <- 0 to print_predict.length - 1) {
			println(print_predict(i)._1 + "\t" + print_predict(i)._2)
		}
		//统计分类准确率
		val accury = 1.0 * prediction.filter(x => x._1 == x._2).count() / testda.count()
		println("正确率：" + accury)

	}
}
