package FirstTry

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
	* 零件一：初始化环境
	* Created by MingDong on 2016/8/29.
	*/
class Parts1 {

	var sc:SparkContext = null;
	def sparkConf(master: String): SparkSession = {
		val spark = SparkSession
			.builder().master(master)
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		sc = spark.sparkContext
		return spark
	}

	def hbaseConf(tablename:String): Unit = {

	}
}
