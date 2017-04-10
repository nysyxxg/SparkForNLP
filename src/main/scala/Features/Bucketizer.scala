package Features

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
	* Created by MingDong on 2016/9/27.
	*/
object Bucketizer {
	val spark = SparkSession
		.builder().master("yarn")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	val sc = spark.sparkContext
	import spark.implicits._
	def main(args: Array[String]): Unit = {
		val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

		val data = Array(-0.5, -0.3, 0.0, 0.2)
		val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

		val bucketizer = new Bucketizer()
			.setInputCol("features")
			.setOutputCol("bucketedFeatures")
			.setSplits(splits)

		// Transform original data into its bucket index.
		val bucketedData = bucketizer.transform(dataFrame)
		bucketedData.show()
	}
}
