package spark

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
	* Created by MingDong on 2016/8/22.
	*/
object Test1 {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local")
		  .appName("Test1")
		  .config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		  .getOrCreate()
		val test = spark.createDataFrame(Seq(
			(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
			(0.0, Vectors.dense(3.0, 2.0, -0.1)),
			(1.0, Vectors.dense(0.0, 2.2, -1.5))
		)).toDF("label", "features")
		println(test)
		test.select("features", "label")
			.collect()
			.foreach { case Row(features: Vector, label: Double, prob: Vector) =>
				println(s"($features, $label) -> prob=$prob")
			}
		/*test.foreach(case Row(features: Vector, label: Double) =>
		println(s"($features, $label)"))*/

	}

}
