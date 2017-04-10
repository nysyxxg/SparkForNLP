package socket;


import org.apache.spark.ml.PipelineModel;

import java.io.*;

import java.net.*;

/**
 * Created by MingDong on 2016/9/7.
 */
public class Server {
    static PipelineModel model = getModel.model();
    public static void main(String[] args) throws IOException {

        ServerSocket server = new ServerSocket(5678);

        Socket client = server.accept();

        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

        PrintWriter out = new PrintWriter(client.getOutputStream());


        while (true) {

            String str = in.readLine();

            System.out.println(str);
           String p = getModel.del(str,model);

            out.println(p);

            out.flush();
            if(str.equals("end"))

                break;

        }

        client.close();

    }
}
