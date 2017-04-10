package spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
	* Created by MingDong on 2016/9/1.
	*/
object Test5 {

	case class RawDataRecord(label: String, message: String)

	final val VECTOR_SIZE = 100

	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext

		//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
		val parsedRDD = sc.textFile("file:///e:/text/").map {
			x =>
				var data = x.split(",")
				(data(0), data(1).split(" "))
		}

		val msgDF = spark.createDataFrame(parsedRDD).toDF("label", "message")
		val labelIndexer = new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(msgDF)

		val word2Vec = new Word2Vec()
			.setInputCol("message")
			.setOutputCol("features")
			.setVectorSize(VECTOR_SIZE)
			.setMinCount(1)

		val layers = Array[Int](VECTOR_SIZE, 14, 7, 14)
		val mlpc = new MultilayerPerceptronClassifier()
			.setLayers(layers)
			.setBlockSize(512)
			.setSeed(1234L)
			.setMaxIter(100)
			.setFeaturesCol("features")
			.setLabelCol("indexedLabel")
			.setPredictionCol("prediction")

		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)

		val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
		val model = pipeline.fit(msgDF)
		model.save("hdfs://master1:8020/model/")
	}
}