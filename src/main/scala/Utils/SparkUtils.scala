package Utils


import java.util.ArrayList

import org.apache.spark.SparkContext
import collection.JavaConversions._
/**
	* Created by MingDong on 2016/8/29.
	*/
class SparkUtils {
	def getDicList(sc: SparkContext): java.util.List[String] = {

		val list: java.util.List[String] = Seq("的", "SJ002")

		val dic = sc.textFile("e://test.txt")
		//.map{line => var data = line}.toDF("")
		//s = Arrays.asList(dic)
		return list
	}

	def main(args: Array[String]): Unit = {
		val arr = new Array[String](5)
		arr.update(0,(0.0,1.0,0.0).toString())
		arr.update(0,(1.0,0.0,0.0).toString())
		arr.update(0,(0.0,0.0,1.0).toString())
		arr.update(0,(1.0,0.0,0.0).toString())
		arr.update(0,(0.0,1.0,0.0).toString())
		arr.update(0,(1.0,0.0,0.0).toString())
		arr.update(0,(0.0,0.0,1.0).toString())
		arr.update(0,(0.0,1.0,1.0).toString())

		for(i<-0 to arr.length-1){
			println(arr(i))
		}
	}
}
