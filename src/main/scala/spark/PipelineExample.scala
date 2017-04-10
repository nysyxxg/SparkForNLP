package spark

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession

/**
	* Created by MingDong on 2016/8/22.
	*/
object PipelineExample {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		val spark = SparkSession
			.builder.master("local")
			.appName("PipelineExample")
			  .config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()

		// $example on$
		// Prepare training documents from a list of (id, text, label) tuples.
		val training = spark.createDataFrame(Seq(
			(0L, "a b c d e spark", 1.0),
			(1L, "b d", 0.0),
			(2L, "spark f g h", 1.0),
			(3L, "hadoop mapreduce", 0.0)
		)).toDF("id", "text", "label")

		// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
		val tokenizer = new Tokenizer()
			.setInputCol("text")
			.setOutputCol("words")
		val hashingTF = new HashingTF()
			.setNumFeatures(1000)
			.setInputCol(tokenizer.getOutputCol)
			.setOutputCol("features")
		val lr = new LogisticRegression()
			.setMaxIter(10)
			.setRegParam(0.01)
		val pipeline = new Pipeline()
			.setStages(Array(tokenizer, hashingTF, lr))

		// Fit the pipeline to training documents.
		val model = pipeline.fit(training)
		// Prepare test documents, which are unlabeled (id, text) tuples.
		val test = spark.createDataFrame(Seq(
			(4L, "spark i j k"),
			(5L, "l m n"),
			(6L, "mapreduce spark"),
			(7L, "apache hadoop")
		)).toDF("id", "text")

		// Make predictions on test documents.
		model.transform(test)
			.select("id", "text", "probability", "prediction")
			.collect()
			.foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
				println(s"($id, $text) --> prob=$prob, prediction=$prediction")
			}
		// $example off$

		spark.stop()
	}
}
