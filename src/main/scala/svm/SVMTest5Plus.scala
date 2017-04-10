package svm

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{ml, mllib}

/**
	* 用TF-IDF值向量化数据来做特征完成的SVM情感分类
	* Created by MingDong on 2016/9/1.
	*/
object SVMTest5Plus {

	case class RawDataRecord(category: String, text: String)

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val srcRDD = sc.textFile("file:///e:/svm4.txt").map {
			x =>
				var data = x.split(":")
				RawDataRecord(data(0), data(1))
		}

		val splits = srcRDD.randomSplit(Array(0.7, 0.3), seed = 11L)
		var trainingDF = splits(0).toDF()
		var testDF = splits(1).toDF()

		//将词语转换成数组
		var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
		var wordsData = tokenizer.transform(trainingDF)


		//计算每个词在文档中的词频
		var hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("rawFeatures")
		var featurizedData = hashingTF.transform(wordsData)


		//计算每个词的IDF
		var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		var idfModel = idf.fit(featurizedData)
		//TF-IDF
		var rescaledData = idfModel.transform(featurizedData)

		//转换成SVM的输入格式
		var trainDataRdd = rescaledData.select($"category", $"features").map {
			case Row(label: String, features:ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.split(",")(2).toDouble, ml.linalg.Vectors.dense(features.toArray))
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
					ml.feature.LabeledPoint(label.split(",")(2).toDouble, ml.linalg.Vectors.dense(features.toArray))
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
		val print_predict = prediction.take(20)
		println("prediction" + "\t" + "label")
		for (i <- 0 to print_predict.length - 1) {
			println(print_predict(i)._1 + "\t" + print_predict(i)._2)
		}
		//统计分类准确率
		val accury = 1.0 * prediction.filter(x => x._1 == x._2).count() / testda.count()
		println("正确率：" + accury)

	}
}
