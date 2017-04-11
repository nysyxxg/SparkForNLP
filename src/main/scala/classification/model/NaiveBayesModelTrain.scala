package classification.model

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
	* Created by MingDong on 2016/9/1.
	*/
object NaiveBayesModelTrain {

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
		val srcRDD = sc.textFile("file:///e:/text/").map {
			x =>
				var data = x.split(",")
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

		val splits = srcRDD.randomSplit(Array(0.7, 0.3), seed = 1234L)
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
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}

		//训练模型
		val model = new NaiveBayes().fit(trainDataRdd)
		//测试数据集，做同样的特征表示及格式转换
		var testwordsData = tokenizer.transform(testDF)
		var testfeaturizedData = hashingTF.transform(testwordsData)
		var testrescaledData = idfModel.transform(testfeaturizedData)

		var testDataRdd = testrescaledData.select($"category", $"features").map {
			case Row(label: String, features: Vector) =>
				LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
		}

		//对测试数据集使用训练模型进行分类预测
		val testpredictionAndLabel = model.transform(testDataRdd) // testDataRdd.map(p => (model.transform(p.features), p.label))
		testpredictionAndLabel.show()
		//统计分类准确率
		//	var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
		val evaluator = new MulticlassClassificationEvaluator()
			.setLabelCol("label")
			.setPredictionCol("prediction")
			.setMetricName("accuracy")
		val accuracy = evaluator.evaluate(testpredictionAndLabel)
		println("output5：")
		println(s"Test Error : ${1 - accuracy}")

		srcRDD.take(1).foreach(println)
	}
}
