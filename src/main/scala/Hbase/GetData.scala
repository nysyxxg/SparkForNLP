package Hbase

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Created by MingDong on 2016/8/23.
	*/
object GetData {
	def main(args: Array[String]): Unit = {

		val hconf = HBaseConfiguration.create()
		hconf.set(TableInputFormat.INPUT_TABLE, "test")
	//	hconf.set(TableInputFormat.SCAN_ROW_START, "1430203560000")
	//	hconf.set(TableInputFormat.SCAN_ROW_STOP, "1430203665000")

		//val sc: SparkContext = new SparkContext(conf)
		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()

		val sc = spark.sparkContext
		import spark.implicits._
		// 从数据源获取数据
		val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
		hbaseRDD.foreach(println)
		// 将数据映射为表  也就是将 RDD转化为 dataframe schema
//		val data = {
//			hbaseRDD.map(r => (Bytes.toString(r._1.get()), //得到rowKey
//				Bytes.toString(r._2.getValue(Bytes.toBytes("article"), Bytes.toBytes("title"))) //得到某一列数据
//				)).toDF("id", "content")
//		}
//		val text = data.select("id", "content")
//	//	val word = text.map(x => ToAnalysis.parse(x.getAs("content")))
//	//	println(word)
//		data.select("id", "content").take(4).foreach(println) //打印前4条数据

	/*	data.select("id", "content").collect().foreach { //打印所有
			case Row(id: String, content: String) =>
			println(s"$id -> ${ToAnalysis.parse(content)}")
		}*/

	/*	data.createOrReplaceTempView("table")

		val result = spark.sql("select content from table")
			result.show()
		val text = result.map(case Row(content:String) => println() )*/


	}
}
