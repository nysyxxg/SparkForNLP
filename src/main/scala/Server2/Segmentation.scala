package Server2

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, _}
import scala.collection.JavaConversions._
/**
	* Created by MingDong on 2016/10/1.
	*/
object Segmentation {
	private val spark = SparkSession
		.builder().master("local[2]")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._
	def del(rdd:RDD[LoadModel.RawDataRecord]): DataFrame = {

		val msgDF = spark.createDataFrame(rdd).toDF()

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
			val words = for (i <- Range(0, 30)) yield filter.get(i).getName
			words.mkString(" ") //词之间以空格分割
		}
		}.toDF("words")
		data
	}
}
