package rpc

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration._

/**
	* Created by MingDong on 2016/9/2.
	*/
class Client1 extends Actor {
	//远程Actor
	var remoteActor: ActorSelection = null
	//当前Actor
	var localActor: akka.actor.ActorRef = null

	@throws[Exception](classOf[Exception])
	override def preStart(): Unit = {
		remoteActor = context.actorSelection("akka.tcp://local:9999/user/server")
		println("远程服务端地址 : " + remoteActor)
	}

	override def receive: Receive = {
		//接收到消息类型为AkkaMessage后，将消息转发至远程Actor
		case msg: AkkaMessage1 => {
			println("客户端发送消息 : " + msg)
			this.localActor = sender()
			remoteActor ! msg
		}
		//接收到远程Actor发送的消息类型为Response，响应
		case res: Response1 => {
			localActor ! res
		}
		case _ => println("客户端不支持的消息类型 .. ")

	}
}

object Client1 {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder().master("local")
			.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
			.getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._

		val clientSystem = ActorSystem("ClientSystem", ConfigFactory.parseString(
			"""
      akka {
       actor {
          provider = "akka.remote.RemoteActorRefProvider"
        }
      }
			"""))
		var client = clientSystem.actorOf(Props[Client1])
		val msgs = Array[AkkaMessage](AkkaMessage("message1"), AkkaMessage("message2"), AkkaMessage("message3"), AkkaMessage("message4"))

		implicit val timeout = Timeout(100 seconds)

		/*msgs.foreach { x =>
			val future = client ? x
			val result = Await.result(future, timeout.duration).asInstanceOf[Response]
			println("收到的反馈： " + result)
		}
*/
		val str: String = "3.0,中国 卫生部 官员 24日 2005 年底 中国 报告 尘肺病 病人 累计 已超过 60万例 职业病 整体 防治 形势严峻 卫生部 副部长 当日 国家 职业 卫生 示范 企业 授牌 企业 职业 卫生 交流 大会 上说 中国 各类 急性 职业 中毒 事故 发生 200 多起 上千人 中毒 直接经济损失 上百 亿元 职业病 病人 量大 发病率 较高 经济损失 影响 恶劣 卫生部 24日 公布 2005年 卫生部 收到 全国 30个 自治区 直辖市 不包括 西藏 各类 职业病 报告 12212例 尘肺病 病例 报告 9173例 75.11 矽肺 煤工尘肺 中国 最主要 尘肺病 尘肺病 发病 工龄 缩短 去年 报告 尘肺病 病人 最短 时间 三个月 平均 发病 年龄 40.9岁 最小 发病 年龄 20岁 政府部门 执法不严 监督 企业 生产水平 不高 技术 设备 落后 职业 卫生 原因 原因是 企业 法制观念 淡薄 社会责任 缺位 缺乏 维护 职工 健康 意识 职工 合法权益 保障 提高 企业 职业 卫生 工作 重视 卫生部 国家安全 生产 监督管理 总局 中华全国总工会 24日 在京 选出 56家 国家级 职业 卫生 工作 示范 企业 希望 企业 社会 推广 职业 病防治 经验 促使 企业 作好 职业 卫生 工作 保护 劳动者 健康 "

		/*val test:RDD[String] = sc.parallelize(Seq(str))
		val testRDD = test.map { x => var data = x.split(",")
			(data(0), data(1).split(" "))
		}
		val testDF = spark.createDataFrame(testRDD).toDF("label", "message")
		val send = AkkaMessage1(testDF)
		val future = client ? send
		val result = Await.result(future, timeout.duration).asInstanceOf[Response]
		println("收到的反馈： " + result)*/

		val send ="我，是，中，国，人"
		clientSystem.shutdown()
	}
}
