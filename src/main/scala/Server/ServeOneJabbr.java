package Server;

import org.apache.spark.ml.PipelineModel;
import socket.getModel;

import java.io.*;
import java.net.Socket;

/**
 * Created by MingDong on 2016/9/8.
 */
public class ServeOneJabbr extends Thread {
    private Socket socket = null;
    private BufferedReader br = null;
    private PrintWriter pw = null;
    private PipelineModel model = null;

    public ServeOneJabbr(Socket s,PipelineModel m) {
        socket = s;
        model = m;
        try {
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
            start();
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true) {
            String str;
            try {
                str = br.readLine();
                if (str.equals("END")) {
                    br.close();
                    pw.close();
                    socket.close();
                    break;
                }
                System.out.println("Client Socket Message:" + str);
                String p = getModel.del(str,model);

                pw.println(p);
                pw.flush();
            } catch (Exception e) {
                try {
                    br.close();
                    pw.close();
                    socket.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
    }
}
