package Server3;

import Server2.LoadModel;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by MingDong on 2016/9/30.
 */
public class PolarServer3 {

  /*  //获得是否中立的tfidfModel
    static PipelineModel tfidfmodel1 = LoadModel3.tfidfModel("hdfs://master1:8020/svm/tfidfModel1");
            //("hdfs://mycluster:8020/svm/tfidfModel1/");
    //获得正负面的tfidfModel
    static PipelineModel tfidfmodel2 = LoadModel3.tfidfModel("hdfs://master1:8020/svm/tfidfModel2/");
    //获得是否中立的SVMModel
    static SVMModel svmmodel1 = LoadModel3.svmModel("hdfs://master1:8020/svm/svmModel1/");
    //获的正负面的SVMModel
    static SVMModel svmmodel2 = LoadModel3.svmModel("hdfs://master1:8020/svm/svmModel2/");*/


    public static void main(String[] args) {
        //获得是否中立的tfidfModel
       PipelineModel tfidfmodel1 = LoadModel3.tfidfModel("hdfs://master1:8020/svm/tfidfModel1");
        //("hdfs://mycluster:8020/svm/tfidfModel1/");
        //获得正负面的tfidfModel
         PipelineModel tfidfmodel2 = LoadModel3.tfidfModel("hdfs://master1:8020/svm/tfidfModel2/");
        //获得是否中立的SVMModel
         SVMModel svmmodel1 = LoadModel3.svmModel("hdfs://master1:8020/svm/svmModel1/");
        //获的正负面的SVMModel
         SVMModel svmmodel2 = LoadModel3.svmModel("hdfs://master1:8020/svm/svmModel2/");
        ServerSocket s = null;
        Socket socket = null;
        try {
            s = new ServerSocket(9997);
            //等待新请求、否则一直阻塞
            while (true) {
                socket = s.accept();
                System.out.println("socket:" + socket);
                new PolarServerOneJabbr3(socket,tfidfmodel1,tfidfmodel2,svmmodel1,svmmodel2);
            }
        } catch (Exception e) {
            try {
                socket.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        } finally {
            try {
                s.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }


    }
}
