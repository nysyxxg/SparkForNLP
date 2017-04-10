package rpc

import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.stat.regression.ModelSpecificationException
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AkkaMessage1(message: DataFrame)
case class Response1(response: Any)
/**
	* Created by MingDong on 2016/9/2.
	*/

class Server1 extends Actor{


	override def receive: Receive = {
		//接收到的消息类型为AkkaMessage，则在前面加上response_，返回给sender

		case msg: AkkaMessage1 => {

			println("服务端收到消息: " + msg.message)

			sender ! Response1(PipelineModel.load("file:///e://nodel1").transform(msg.message))
		}
		case _ => println("服务端不支持的消息类型 .. ")
	}
}
object Server1 {
	final val VECTOR_SIZE = 100
	def main(args: Array[String]): Unit = {
		val serverSystem = ActorSystem("Mingdong", ConfigFactory.parseString("""
      akka {
       actor {
          provider = "akka.remote.RemoteActorRefProvider"
        }
        remote {
          enabled-transports = ["akka.remote.netty.tcp"]
          netty.tcp {
            hostname = "127.0.0.1"
            port = 2555
          }
        }
      } """))


			/*val spark = SparkSession
				.builder().master("local")
				.config("spark.sql.warehouse.dir", "file:///:D:\\IdeaProjects\\sparktest\\spark-warehouse")
				.getOrCreate()
			val sc = spark.sparkContext
			import spark.implicits._

			//将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
			val parsedRDD = sc.textFile("file:///e:/text/").map {
				x =>
					var data = x.split(",")
					(data(0), data(1).split(" "))
			}

			val msgDF = spark.createDataFrame(parsedRDD).toDF("label", "message")
			val labelIndexer = new StringIndexer()
				.setInputCol("label")
				.setOutputCol("indexedLabel")
				.fit(msgDF)

			val word2Vec = new Word2Vec()
				.setInputCol("message")
				.setOutputCol("features")
				.setVectorSize(VECTOR_SIZE)
				.setMinCount(1)

			val layers = Array[Int](VECTOR_SIZE, 10, 5, 10)
			val mlpc = new MultilayerPerceptronClassifier()
				.setLayers(layers)
				.setBlockSize(512)
				.setSeed(1234L)
				.setMaxIter(100)
				.setFeaturesCol("features")
				.setLabelCol("indexedLabel")
				.setPredictionCol("prediction")

			val labelConverter = new IndexToString()
				.setInputCol("prediction")
				.setOutputCol("predictedLabel")
				.setLabels(labelIndexer.labels)

			val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

			val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
			val model = pipeline.fit(trainingData)
		model.save("file:///e://model1")*/
		serverSystem.actorOf(Props[Server1], "server")
	}
}
