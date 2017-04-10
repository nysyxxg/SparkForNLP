package Utils

import java.util

import spire.std.boolean

/**
	* Created by MingDong on 2016/9/26.
	*/
object Test {
	def main(args: Array[String]): Unit = {
		//val arr = new Array[String](8)
		val arr:java.util.List[String] = new util.ArrayList[String]();
		arr.add( (0.0, 1.0, 0.0).toString())
		arr.add((1.0, 0.0, 0.0).toString())
		arr.add((0.0, 0.0, 1.0).toString())
		arr.add((1.0, 0.0, 0.0).toString())
		arr.add((0.0, 1.0, 0.0).toString())
		arr.add((1.0, 0.0, 0.0).toString())
		arr.add((0.0, 0.0, 1.0).toString())
		arr.add((0.0, 0.0, 1.0).toString())

		for (i <- 0 to 7) {
			println(arr.get(i))
		}
		println(arr.size())
println(arr.toString)
	}
}
