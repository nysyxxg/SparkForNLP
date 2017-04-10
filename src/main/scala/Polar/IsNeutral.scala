package Polar

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{ml, mllib}

/**
	* 用TF-IDF来向量化数据做特征完成的完整版SVM情感分类1-判断是否中立面
	* Created by MingDong on 2016/9/1.
	*/
object IsNeutral {

	private val spark = SparkSession
		.builder()
		.config("spark.sql.warehouse.dir", "IdeaProjects/sparktest/spark-warehouse")
	  .master("spark://172.16.110.105:7077")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._


	case class RawDataRecord(lable: String, message: Array[String])

	def main(args: Array[String]): Unit = {
		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val parsedRDD = sc.textFile("hdfs://master1:8020/Mdsd-bigdata-DPMS/Resource/Corpus/Polar/isNeutral.txt").map {
			x =>
				val data = x.split(",")
				RawDataRecord(data(0), data(1).split(" "))
		}

		val msgDF = spark.createDataFrame(parsedRDD).toDF()
		val Array(training1, test1) = msgDF.randomSplit(Array(0.9, 0.1), seed = 132L)

		val model = getModel1(training1)
		//model.save(args(1))
		//向量化语料集
		val training2 = model.transform(training1)
		//向量化测试集
		val test2 = model.transform(test1)
			//得到是否中立面的SVMmodel
			val smodel1 = getModel2(training2)
		smodel1.save(sc,args(2))
			//得到是否中立面的预测值和真实值（预测值，真实值）
			val prediction2 = TestModel(smodel1, test2)
	}

	//得到文本向量化的model
	def getModel1(dataDF: DataFrame): PipelineModel = {
		val lables = dataDF.select("lable")
		val labelIndexer = new StringIndexer()
			.setInputCol("lable")
			.setOutputCol("indexedLabel")
			.fit(lables)
		val hashingTF = new HashingTF()
			.setInputCol("message")
			.setOutputCol("rawFeatures")
			.setNumFeatures(1<<16)
		val idf = new IDF()
			.setInputCol("rawFeatures")
			.setOutputCol("features")
			//.setOutputCol("features1")
		val selector = new ChiSqSelector()
			.setNumTopFeatures(20)
			.setFeaturesCol("features1")
			.setLabelCol("indexedLabel")
			.setOutputCol("features")
		val pipeline = new Pipeline().setStages(Array(labelIndexer, hashingTF, idf))
		val model1 = pipeline.fit(dataDF)
		model1
	}

	//得到SVM分类器的model
	def getModel2(dataDF: DataFrame): SVMModel = {
		val DataRdd = dataDF.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		DataRdd.foreach(println)
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val model2 = SVMWithSGD.train(MllibDataRdd, 100)
		model2
	}

	//对最后的模型进行测试
	def TestModel(model: SVMModel, test: DataFrame) {
		val DataRdd = test.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val pres = MllibDataRdd.map { point =>
			val score = model.predict(point.features)
			(score,point.label)
		}
		val p = pres.collect()
		println("prediction" + "\t" + "label")
		for (i <- 0 to p.length - 1) {
			println(p(i)._1 + "\t" + p(i)._2)
		}
		val accury = 1.0 * pres.filter(x => x._1 == x._2).count() / test.count()
		println("正确率：" + accury)
	}
}