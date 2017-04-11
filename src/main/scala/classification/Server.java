package classification;

import org.apache.spark.ml.PipelineModel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Created by MingDong on 2016/9/8.
 */
public class Server {
    static PipelineModel model = getModel.model();
    public static void main(String[] args) {
        ServerSocket s = null;
        Socket socket = null;
        try {
            s = new ServerSocket(9930);
            //等待新请求、否则一直阻塞
            while (true) {
                socket = s.accept();
                System.out.println("socket:" + socket);
               // new Server.ServeOneJabbr(socket,model);
                new ServeOneJabbr(socket,model);

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
