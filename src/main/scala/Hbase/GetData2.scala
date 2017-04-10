package Hbase

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import java.util.Arrays

/**
	* Created by MingDong on 2016/8/23.
	*/
object GetData2 {
	def main(args: Array[String]): Unit = {

		val hconf = HBaseConfiguration.create()
		hconf.set(TableInputFormat.INPUT_TABLE, "Testdata")
		hconf.set(TableInputFormat.SCAN_ROW_START, "1430203560000")
		hconf.set(TableInputFormat.SCAN_ROW_STOP, "1430203665000")

		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		// 从数据源获取数据
		val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
		//hbaseRDD.foreach(println)

		//对表中的数据项进行分词
		val text = hbaseRDD.map(r => Bytes.toString(r._2.getValue(Bytes.toBytes("article"), Bytes.toBytes("title")))) //得到数据的内容RDD
		val word = text.map { x =>
			val temp = ToAnalysis.parse(x)
			FilterModifWord.insertStopWords(Arrays.asList("r","n"))
			//加入停用词性
			FilterModifWord.insertStopNatures("w",null,"ns","r","u","e")
			val filter = FilterModifWord.modifResult(temp)
			val key = for (i <- Range(0, filter.size())) yield filter.get(i).getName
			key.mkString("\t")
		}
		//word.foreach(println)
		val id = hbaseRDD.map(r => (Bytes.toString(r._1.get())))//得到数据的rowkey的RDD
		//id.foreach(println)
		val result = id.union(word).collect()
		result.foreach(println)

		// 将数据映射为表  也就是将 RDD转化为 dataframe schema
		/*val data = spark.createDataFrame(
			id.map(x => ),
			word
		).toDF("id", "content")

		//val t = ToAnalysis.parse(data.select("content").toString())
		//data.select("id", "content").take(4).foreach(println) //打印前4条数据
		data.select("id", "content").collect().foreach { //打印所有
			case Row(id: String, content: String) =>
			println(s"$id -> ${ToAnalysis.parse(content)}")
		}
*/
		/*	data.createOrReplaceTempView("table")

			val result = spark.sql("select content from table")
				result.show()
			val text = result.map(case Row(content:String) => println() )*/


	}
}
