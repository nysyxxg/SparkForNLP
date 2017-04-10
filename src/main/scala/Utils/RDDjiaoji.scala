package Utils

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ml, mllib}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql._
import scala.collection.JavaConversions._

/**
	* 暂时废弃，时间因素不能深究了
	* Created by MingDong on 2016/9/27.
	*/
object RDDjiaoji {
	private val spark = SparkSession
		.builder().master("local[2]")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._

	case class RawDataRecord(lable: String, message: String)

	case class DicDataRecord(word: String, value: String)

	def main(args: Array[String]): Unit = {
		val parsedRDD = sc.textFile("file:///e:/svm/svm4.txt",3).map {
			x =>
				val data = x.split(",")
				RawDataRecord(data(0), data(1))
		}

		val msgDF = spark.createDataFrame(parsedRDD).toDF()

		//(1)加载自定义词典（向原有补充新词,可加载多个）（后续考虑去重）
		sc.textFile("file:///e://svm/dic.txt")
			.map(_.split("	")).collect()
			.foreach(attributes => UserDefineLibrary.insertWord(attributes(0),attributes(1) , 1000))
		//(2)加载停用词词表
		val stopwords: java.util.List[String] = sc.textFile("file:///e://stopword.dic").collect().toSeq
		FilterModifWord.insertStopWords(stopwords)
		//(3)根据词性去停用词
		FilterModifWord.insertStopNatures("w", null)

		//零件四：分词
		val data: DataFrame = msgDF.select("message").map { case Row(text: String) => {
			//分词
			val temp = ToAnalysis.parse(text)
			val filter = FilterModifWord.modifResult(temp)
			val words = for (i <- Range(0, filter.size())) yield filter.get(i).getName
			words.mkString(" ") //词之间以空格分割
		}
		}.toDF("words")
		//零件五：打印前5条预览
		data.select("words").collect().take(5).foreach{case Row(wo:String)=>
			println(wo)
		//println(wo.replaceAll("// ","").replace(" //",""))
		}
	}
}
