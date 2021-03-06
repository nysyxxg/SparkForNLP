package Polar;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.classification.SVMModel;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MingDong on 2016/9/30.
 */
public class PolarServerOneJabbr extends Thread {
    private Socket socket = null;
    private BufferedReader br = null;
    private PrintWriter pw = null;

    private String fenCiPath = null;
    private String stopWordPath = null;

    private PipelineModel tfidfModel1 = null;
    private SVMModel svmModel1 = null;

    private PipelineModel tfidfModel2 = null;
    private SVMModel svmModel2 = null;

    boolean flag = false;

    StringBuffer result = new StringBuffer();

    public PolarServerOneJabbr(Socket s,String fenciPath,String stopwordPath, PipelineModel tfidf1, PipelineModel tfidf2, SVMModel svm1, SVMModel svm2) {
        socket = s;
        fenCiPath = fenciPath;
        stopWordPath = stopwordPath;

        tfidfModel1 = tfidf1;
        tfidfModel2 = tfidf2;

        svmModel1 = svm1;
        svmModel2 = svm2;
        try {
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
            start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> dataUtil(String str) {
        List<String> list = new ArrayList<String>();

        //String strs[] = str.split("@@");
       // for (String s : strs) {
            //System.out.println(s);
            //String result
         list= LoadModel.pric(str, fenCiPath, stopWordPath, tfidfModel1, tfidfModel2, svmModel1, svmModel2);
           // list.add(result);
        //}
        return list;
    }

    @Override
    public void run() {
        while (true) {
            String strs;
            try {
                strs = br.readLine();
                if (strs.equals("END")) {
                    br.close();
                    pw.close();
                    socket.close();
                    break;
                }
                System.out.println("Client Socket Message:" + strs);
                List<String> li = dataUtil(strs);
               // System.out.println(li.size());
                for (String s : li) {
                    if (flag) {
                        result.append("@@");
                    } else {
                        flag = true;
                    }
                    result.append(s);
                }
                pw.println(result);
                pw.flush();
                li.clear();
            } catch (Exception e) {
                try {
                    br.close();
                    pw.close();
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}
