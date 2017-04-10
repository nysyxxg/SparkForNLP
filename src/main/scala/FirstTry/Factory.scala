package FirstTry

import org.ansj.library.UserDefineLibrary

/**
	* Created by MingDong on 2016/8/29.
	*/
object Factory {
	def main(args: Array[String]): Unit = {
		//零件一：初始化环境
		val conf = new Parts1()
		val spark = conf.sparkConf("local")
		val sc = conf.sc

	val rdd = sc.textFile("E://test.txt")
		rdd.foreach(println)
			rdd.map(word => UserDefineLibrary.insertWord(word,"userDefine", 1000))


		//零件二：获取数据
		//零件三：加载分词配置
		//零件四：分词
		//零件五：打印预览
	}
}
