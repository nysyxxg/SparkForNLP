package socket

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
/**
	* Created by MingDong on 2016/9/7.
	*/
object getModel {
	val spark = SparkSession
		.builder().master("spark://192.168.1.109:7077")
		.config("spark.sql.warehouse.dir", "park-warehouse")
		.getOrCreate()
	val sc = spark.sparkContext

	import spark.implicits._

	def model(): PipelineModel = {

		val model = PipelineModel.load("hdfs://mycluster:8020/model/")

		return model
	}

	def del(str: String,model: PipelineModel): String = {
		val test: RDD[String] = sc.parallelize(Seq(str))
		val testRDD = test.map { x =>
 			(0, x.split(" "))
		}
		val testDF = spark.createDataFrame(testRDD).toDF("label", "message")
		val p = model.transform(testDF).select("predictedLabel").rdd.map{case Row(label: String) =>
			label}
		return p.collect().toSeq.get(0)
	}

	def main(args: Array[String]): Unit = {
		//val str:String = "中国 卫生部 官员 24日 2005 年底 中国 报告 尘肺病 病人 累计 已超过 60万例 职业病 整体 防治 形势严峻 卫生部 副部长 当日 国家 职业 卫生 示范 企业 授牌 企业 职业 卫生 交流 大会 上说 中国 各类 急性 职业 中毒 事故 发生 200 多起 上千人 中毒 直接经济损失 上百 亿元 职业病 病人 量大 发病率 较高 经济损失 影响 恶劣 卫生部 24日 公布 2005年 卫生部 收到 全国 30个 自治区 直辖市 不包括 西藏 各类 职业病 报告 12212例 尘肺病 病例 报告 9173例 75.11 矽肺 煤工尘肺 中国 最主要 尘肺病 尘肺病 发病 工龄 缩短 去年 报告 尘肺病 病人 最短 时间 三个月 平均 发病 年龄 40.9岁 最小 发病 年龄 20岁 政府部门 执法不严 监督 企业 生产水平 不高 技术 设备 落后 职业 卫生 原因 原因是 企业 法制观念 淡薄 社会责任 缺位 缺乏 维护 职工 健康 意识 职工 合法权益 保障 提高 企业 职业 卫生 工作 重视 卫生部 国家安全 生产 监督管理 总局 中华全国总工会 24日 在京 选出 56家 国家级 职业 卫生 工作 示范 企业 希望 企业 社会 推广 职业 病防治 经验 促使 企业 作好 职业 卫生 工作 保护 劳动者 健康 ";
		/*val model: PipelineModel = getModel.model()

		val s = getModel.del(str,model)*/
		val str :String = "中国卫生部官员 24日 2005 年底 中国 报告 尘肺病 病人 累计 已超过 60万例 职业病 整体 防治 形势严峻 卫生部 副部长 当日 国家 职业 卫生 示范 企业 授牌 企业 职业 卫生 交流 大会 上说 中国 各类 急性 职业 中毒 事故 发生 200 多起 上千人 中毒 直接经济损失 上百 亿元 职业病 病人 量大 发病率 较高 经济损失 影响 恶劣 卫生部 24日 公布 2005年 卫生部 收到 全国 30个 自治区 直辖市 不包括 西藏 各类 职业病 报告 12212例 尘肺病 病例 报告 9173例 75.11 矽肺 煤工尘肺 中国 最主要 尘肺病 尘肺病 发病 工龄 缩短 去年 报告 尘肺病 病人 最短 时间 三个月 平均 发病 年龄 40.9岁 最小 发病 年龄 20岁 政府部门 执法不严 监督 企业 生产水平 不高 技术 设备 落后 职业 卫生 原因 原因是 企业 法制观念 淡薄社会责任 缺位 缺乏 维护 职工 健康 意识 职工 合法权益 保障 提高 企业 职业 卫生 工作 重视 卫生部 国家安全 生产 监督管理 总局 中华全国总工会 24日 在京 选出 56家 国家级 职业 卫生 工作 示范 企业 希望 企业 社会 推广 职业 病防治经验促使企业作好职业卫生工作保护劳动者健康 ";
		val model = PipelineModel.load("hdfs://mycluster:8020/model/")
		val test: RDD[String] = sc.parallelize(Seq(str))
		test.map {line=>
			val temp = ToAnalysis.parse(line)
			val filter = FilterModifWord.modifResult(temp)
			val words = for (i <- Range(0, filter.size())) yield filter.get(i).getName //.replaceAll(" ","")
			words.mkString("\t") //词之间以制表符分割)
		}
		val testRDD = test.map { x =>
			(0, x.split("\t"))
		}
		val testDF = spark.createDataFrame(testRDD).toDF("label", "message")
		val p = model.transform(testDF).select("predictedLabel").rdd.map{case Row(label: String) =>
			label}
		val s = p.collect().toSeq.get(0)
	//	val we = model.
		println(s)
	}
}
