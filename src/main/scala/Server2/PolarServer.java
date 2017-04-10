package Server2;

import Server.ServeOneJabbr;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by MingDong on 2016/9/30.
 */
public class PolarServer {

    //获得是否中立的tfidfModel
    static PipelineModel tfidfmodel1 = LoadModel.tfidfModel("hdfs://mycluster:8020/svm/tfidfModel1");
            //("hdfs://mycluster:8020/svm/tfidfModel1/");
    //获得正负面的tfidfModel
    static PipelineModel tfidfmodel2 = LoadModel.tfidfModel("hdfs://mycluster:8020/svm/tfidfModel2/");
    //获得是否中立的SVMModel
    static SVMModel svmmodel1 = LoadModel.svmModel("hdfs://mycluster:8020/svm/svmModel1/");
    //获的正负面的SVMModel
    static SVMModel svmmodel2 = LoadModel.svmModel("hdfs://mycluster:8020/svm/svmModel2/");


    public static void main(String[] args) {
        ServerSocket s = null;
        Socket socket = null;
        try {
            s = new ServerSocket(9998);
            //等待新请求、否则一直阻塞
            while (true) {
                socket = s.accept();
                System.out.println("socket:" + socket);
                new PolarServerOneJabbr(socket,tfidfmodel1,tfidfmodel2,svmmodel1,svmmodel2);
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
