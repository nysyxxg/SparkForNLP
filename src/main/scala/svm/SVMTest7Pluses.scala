package svm

import Server2.LoadModel
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.{ml, mllib}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

/**
	* Created by MingDong on 2016/9/7.
	*/
object SVMTest7Pluses {
	private val spark = SparkSession
		.builder().master("local[2]")
		.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
		.getOrCreate()
	private val sc = spark.sparkContext

	import spark.implicits._

	case class RawDataRecord(lable: String, message: Array[String])

	//加载tf-idf的model
	def tfidfModel(path: String): PipelineModel = {
		val model = PipelineModel.load(path)
		return model
	}

	//加载SVM的model
	def svmModel(path: String): SVMModel = {
		val model = SVMModel.load(sc, path)
		return model
	}

	//得到SVM的预测值
	def del(testDF:DataFrame, model: SVMModel): RDD[String] = {

		val DataRdd = testDF.select($"lable", $"features").map {
			case Row(label: String, features: ml.linalg.Vector) =>
				ml.feature.LabeledPoint(label.toDouble, ml.linalg.Vectors.dense(features.toArray))
		}.rdd
		val MllibDataRdd = DataRdd.map { line => val lable = line.label
			val fea = line.features.toArray
			mllib.regression.LabeledPoint(lable, mllib.linalg.Vectors.dense(fea))
		}
		val pres = MllibDataRdd.map { point =>
			val score = model.predict(point.features)
			score.toString
		}
		//val p = pres.collect()
		//p.toSeq.get(0).toString()
		pres
	}

	def main(args: Array[String]): Unit = {
	//	val str: String = "中国卫生部官员 24日 2005 年底 中国 报告 尘肺病 病人 累计 已超过 60万例 职业病 整体 防治 形势严峻 卫生部 副部长 当日 国家 职业 卫生 示范 企业 授牌 企业 职业 卫生 交流 大会 上说 中国 各类 急性 职业 中毒 事故 发生 200 多起 上千人 中毒 直接经济损失 上百 亿元 职业病 病人 量大 发病率 较高 经济损失 影响 恶劣 卫生部 24日 公布 2005年 卫生部 收到 全国 30个 自治区 直辖市 不包括 西藏 各类 职业病 报告 12212例 尘肺病 病例 报告 9173例 75.11 矽肺 煤工尘肺 中国 最主要 尘肺病 尘肺病 发病 工龄 缩短 去年 报告 尘肺病 病人 最短 时间 三个月 平均 发病 年龄 40.9岁 最小 发病 年龄 20岁 政府部门 执法不严 监督 企业 生产水平 不高 技术 设备 落后 职业 卫生 原因 原因是 企业 法制观念 淡薄社会责任 缺位 缺乏 维护 职工 健康 意识 职工 合法权益 保障 提高 企业 职业 卫生 工作 重视 卫生部 国家安全 生产 监督管理 总局 中华全国总工会 24日 在京 选出 56家 国家级 职业 卫生 工作 示范 企业 希望 企业 社会 推广 职业 病防治经验促使企业作好职业卫生工作保护劳动者健康 ";
	/*	val str:String = "买 房子 排队 抽签 现在 已经 老套 武术 之 乡 佛山 有 新 玩儿 法" +
			"猜拳 剪刀 石头 布 听上去 很 幼稚 人家 卖房 真 就 这么 玩儿 的。 杭州 更 奇葩 打麻将 谁 先 胡牌 谁 先 买房" +
			"你们 让 成都 人民 情何以堪 房市 没有 那么 神秘 跟炒 A股 一个样" +
			"10 万 一平方米 房子 10年 前 想 都 不敢 想 那个 时候 掌柜 我 遇到 一个 非常 有名  经济学家 简直 就是 一个 房价 死空头" +
			"整天 警告 老百姓 中国 房价 跟 日本 当年 走势 很 像 房价 泡沫 已经 走到 当年 日本 奔溃  边沿。" +
			"这个 死 空 头 经济学家 那 几年 走到 哪里 都是 他那 一套 理论 PPT 标点 没有 改一下。" +
			"让人 大跌 眼镜 是 这个 死 空 头 自己 偷偷 买了 好多 套 房子。"*/
		//val str ="当天 沈阳 晚报 沈阳网 记者 就 这个 问题 咨询 中国银行 沈阳 奥体中心 支行 银行 工作 人员 表示 虽然 目前 市面上 已经 很少 见" + "但 第四套 人民币 并未 停止 流通" + "银行 为 ATM机 配款 时 不会 投放 第四套 人民币" + "但 不排除 其他 储户 将 旧版 人民币 存入 ATM机，" + "而 ATM机 属于 存取款 一体机 存款箱 和 取款箱 是 循环 的。"
		val str =""
		//获得是否中立的tfidfModel
		val tfidfModel1 = tfidfModel("file:///e:/svm/tfidfModel1/")
		//获得正负面的tfidfModel
		val tfidfModel2 = tfidfModel("file:///e:/svm/tfidfModel2/")
		//获得是否中立的SVMModel
		val svmModel1 = svmModel("file:///e:/svm/svmModel1/")
		//获的正负面的SVMModel
		val svmModel2 = svmModel("file:///e:/svm/svmModel2/")

		//解析文本

	/*val test: RDD[String] = sc.parallelize(Seq(str))
		val testRDD = test.map { x =>
			RawDataRecord("0", x.split(" "))
		}*/
		val rdd = sc.textFile("file:///e:/svm/test.txt").map{line =>
			val p = line.split(",")
			RawDataRecord("0", p(1).split(" "))
		}
		rdd.collect().foreach(println)
		val testDF = spark.createDataFrame(rdd).toDF()
/*

		testDF.map{case Row(words:String) =>

		}
*/

		//对于是否中立进行预测：
		//(预测tfidf)
		val tfidf1 = tfidfModel1.transform(testDF)
		//(预测中性是或否(1.0 or 0.0))
		val pre1 = del(tfidf1, svmModel1)

		//对于正负进行预测：
		//(预测tfidf)
		val tfidf2 = tfidfModel2.transform(testDF)
		//(预测正或负(1.0 or 0.0))
		val pre2 = del(tfidf2, svmModel2)
		val count = pre1.collect().length
		val p1 = pre1.take(count)
		val p2 = pre2.take(count)
		//val p3 = prediction3.take(count)
		val p = new Array[String](count)
		for (i <- 0 to (count - 1)) {
		//	p.update(i, (p1(i),p2(i)).toString())
			if (p1(i) == "1.0") {
				p.update(i, ("0"))
			} else {
				if (p2(i) == "1.0") {
					p.update(i, ("1"))
				} else {
					p.update(i, ("-1"))
				}
			}
		}
		p.foreach(println)
	}
}
