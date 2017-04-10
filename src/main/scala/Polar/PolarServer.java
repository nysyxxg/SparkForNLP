package Polar;

import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by MingDong on 2016/9/30.
 */
public class PolarServer {


    private Logger logger = Logger.getLogger(PolarServer.class);
    public static void main(String[] args) {
        //分词词表的路径
        String dictPath = args[0];
        //停用词的路径
        String stopWordDict = args[1];
        //获得是否中立的tfidfModel
        PipelineModel tfidfmodel1 = LoadModel.tfidfModel(args[2]);
        //("hdfs://mycluster:8020/svm/tfidfModel1/");
        //获得正负面的tfidfModel
        PipelineModel tfidfmodel2 = LoadModel.tfidfModel(args[3]);
        //获得是否中立的SVMModel
        SVMModel svmmodel1 = LoadModel.svmModel(args[4]);
        //获的正负面的SVMModel
        SVMModel svmmodel2 = LoadModel.svmModel(args[5]);

        ServerSocket s = null;
        Socket socket = null;
        try {
            s = new ServerSocket(Integer.parseInt(args[6]));
            //等待新请求、否则一直阻塞
            while (true) {
                socket = s.accept();
                new PolarServerOneJabbr(socket,dictPath,stopWordDict,tfidfmodel1,tfidfmodel2,svmmodel1,svmmodel2);
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
